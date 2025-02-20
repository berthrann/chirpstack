use anyhow::Result;
use chrono::{TimeDelta, Utc};
use tracing::info;

use lrwn::applayer::{fragmentation, multicastsetup};
use lrwn::region::MacVersion;

use crate::config;
use crate::downlink;
use crate::gpstime::ToGpsTime;
use crate::storage::fields::FuotaJob;
use crate::storage::{device_keys, device_profile, device_queue, fuota, multicast};

pub struct Flow {
    job: fuota::FuotaDeploymentJob,
    fuota_deployment: fuota::FuotaDeployment,
    device_profile: device_profile::DeviceProfile,
}

impl Flow {
    pub async fn handle_job(job: fuota::FuotaDeploymentJob) -> Result<()> {
        let fuota_deployment = fuota::get_deployment(job.fuota_deployment_id.into()).await?;
        let device_profile =
            device_profile::get(&fuota_deployment.device_profile_id.into()).await?;

        let mut flow = Flow {
            job,
            fuota_deployment,
            device_profile,
        };
        flow.dispatch().await
    }

    async fn dispatch(&mut self) -> Result<()> {
        let conf = config::get();
        self.job.attempt_count += 1;

        info!(attempt_count = self.job.attempt_count, "Starting job");

        let resp = match self.job.job {
            FuotaJob::CreateMcGroup => self.create_mc_group().await,
            FuotaJob::AddDevsToMcGroup => self.add_devices_to_multicast_group().await,
            FuotaJob::AddGwsToMcGroup => self.add_gateways_to_multicast_group().await,
            FuotaJob::McGroupSetup => self.multicast_group_setup().await,
            FuotaJob::FragSessionSetup => self.fragmentation_session_setup().await,
            FuotaJob::McSession => self.multicast_session_setup().await,
            FuotaJob::Enqueue => self.enqueue().await,
            FuotaJob::FragStatus => self.fragmentation_status().await,
        };

        match resp {
            Ok(Some(next_job)) => {
                if self.job.job == next_job {
                    // Re-run the same job in the future.
                    let job = self.job.clone();
                    let _ = fuota::update_job(job).await?;
                } else {
                    // Create the next job (which automatically sets the current job to completed).
                    let _ = fuota::create_job(fuota::FuotaDeploymentJob {
                        fuota_deployment_id: self.job.fuota_deployment_id,
                        job: next_job,
                        max_retry_count: match next_job {
                            FuotaJob::FragSessionSetup | FuotaJob::McGroupSetup => {
                                self.fuota_deployment.unicast_max_retry_count
                            }
                            _ => 0,
                        },
                        ..Default::default()
                    })
                    .await?;
                }
            }
            Ok(None) => {
                // No further jobs to execute, set the current job to completed.
                let mut job = self.job.clone();
                job.completed_at = Some(Utc::now());
                let _ = fuota::update_job(job).await?;
            }
            Err(e) => {
                // Re-run the same job in the future.
                let mut job = self.job.clone();
                job.scheduler_run_after = Utc::now() + conf.network.scheduler.interval;
                job.return_msg = format!("Error: {}", e);
                let _ = fuota::update_job(job).await?;
                return Err(e);
            }
        }

        Ok(())
    }

    async fn create_mc_group(&mut self) -> Result<Option<FuotaJob>> {
        // If this job fails, then there is no need to execute the others.
        if self.job.attempt_count - 1 > self.job.max_retry_count {
            return Ok(None);
        }

        info!("Creating multicast-group for FUOTA deployment");

        // Get McAppSKey + McNwkSKey.
        let mc_app_s_key = multicastsetup::v1::get_mc_app_s_key(
            self.fuota_deployment.multicast_key,
            self.fuota_deployment.multicast_addr,
        )?;
        let mc_nwk_s_key = multicastsetup::v1::get_mc_net_s_key(
            self.fuota_deployment.multicast_key,
            self.fuota_deployment.multicast_addr,
        )?;

        let _ = multicast::create(multicast::MulticastGroup {
            id: self.fuota_deployment.id,
            application_id: self.fuota_deployment.application_id,
            name: format!("fuota-{}", self.fuota_deployment.id),
            region: self.device_profile.region,
            mc_addr: self.fuota_deployment.multicast_addr,
            mc_nwk_s_key,
            mc_app_s_key,
            f_cnt: 0,
            group_type: self.fuota_deployment.multicast_group_type.clone(),
            frequency: self.fuota_deployment.multicast_frequency,
            dr: self.fuota_deployment.multicast_dr,
            class_b_ping_slot_nb_k: self.fuota_deployment.multicast_class_b_ping_slot_nb_k,
            class_c_scheduling_type: self.fuota_deployment.multicast_class_c_scheduling_type,
            ..Default::default()
        })
        .await?;

        Ok(Some(FuotaJob::AddDevsToMcGroup))
    }

    async fn add_devices_to_multicast_group(&mut self) -> Result<Option<FuotaJob>> {
        // If this job fails, then there is no need to execute the others.
        if self.job.attempt_count - 1 > self.job.max_retry_count {
            return Ok(None);
        }

        info!("Adding devices to multicast-group");

        let fuota_devices = fuota::get_devices(self.job.fuota_deployment_id.into(), -1, 0).await?;
        for fuota_d in fuota_devices {
            multicast::add_device(&fuota_d.fuota_deployment_id, &fuota_d.dev_eui).await?;
        }

        Ok(Some(FuotaJob::AddGwsToMcGroup))
    }

    async fn add_gateways_to_multicast_group(&mut self) -> Result<Option<FuotaJob>> {
        // If this job fails, then there is no need to execute the others.
        if self.job.attempt_count - 1 > self.job.max_retry_count {
            return Ok(None);
        }

        info!("Adding gateways to multicast-group (if any)");

        let fuota_gws = fuota::get_gateways(self.job.fuota_deployment_id.into(), -1, 0).await?;
        for fuota_gw in fuota_gws {
            multicast::add_gateway(&fuota_gw.fuota_deployment_id, &fuota_gw.gateway_id).await?;
        }

        Ok(Some(FuotaJob::McGroupSetup))
    }

    async fn multicast_group_setup(&mut self) -> Result<Option<FuotaJob>> {
        // Proceed with next step after reaching the max attempts.
        if self.job.attempt_count - 1 > self.job.max_retry_count {
            return Ok(Some(FuotaJob::FragSessionSetup));
        }

        let fuota_devices = fuota::get_devices(self.job.fuota_deployment_id.into(), -1, 0).await?;

        // Filter on devices that have not completed the McGroupSetup.
        let fuota_devices: Vec<fuota::FuotaDeploymentDevice> = fuota_devices
            .into_iter()
            .filter(|d| d.mc_group_setup_completed_at.is_none())
            .collect();

        for fuota_dev in &fuota_devices {
            let dev_keys = device_keys::get(&fuota_dev.dev_eui).await?;
            let mc_root_key = match self.device_profile.mac_version {
                MacVersion::LORAWAN_1_0_0
                | MacVersion::LORAWAN_1_0_1
                | MacVersion::LORAWAN_1_0_2
                | MacVersion::LORAWAN_1_0_3
                | MacVersion::LORAWAN_1_0_4 => {
                    multicastsetup::v1::get_mc_root_key_for_gen_app_key(dev_keys.gen_app_key)?
                }
                MacVersion::LORAWAN_1_1_0 | MacVersion::Latest => {
                    multicastsetup::v1::get_mc_root_key_for_app_key(dev_keys.app_key)?
                }
            };
            let mc_ke_key = multicastsetup::v1::get_mc_ke_key(mc_root_key)?;
            let mc_key_encrypted =
                multicastsetup::v1::encrypt_mc_key(mc_ke_key, self.fuota_deployment.multicast_key);

            let pl = multicastsetup::v1::Payload::McGroupSetupReq(
                multicastsetup::v1::McGroupSetupReqPayload {
                    mc_group_id_header: multicastsetup::v1::McGroupSetupReqPayloadMcGroupIdHeader {
                        mc_group_id: 0,
                    },
                    mc_addr: self.fuota_deployment.multicast_addr,
                    mc_key_encrypted,
                    min_mc_f_count: 0,
                    max_mc_f_count: u32::MAX,
                },
            );

            device_queue::enqueue_item(device_queue::DeviceQueueItem {
                dev_eui: fuota_dev.dev_eui,
                f_port: self.device_profile.app_layer_params.ts005_f_port.into(),
                data: pl.to_vec()?,
                ..Default::default()
            })
            .await?;
        }

        if !fuota_devices.is_empty() {
            // There are devices pending setup, we need to re-run this job.
            self.job.scheduler_run_after =
                Utc::now() + TimeDelta::seconds(self.device_profile.uplink_interval as i64);
            Ok(Some(FuotaJob::McGroupSetup))
        } else {
            // All devices have completed the setup, move on to next job.
            Ok(Some(FuotaJob::FragSessionSetup))
        }
    }

    async fn fragmentation_session_setup(&mut self) -> Result<Option<FuotaJob>> {
        // Proceed with next step after reaching the max attempts.
        if self.job.attempt_count - 1 > self.job.max_retry_count {
            return Ok(Some(FuotaJob::McSession));
        }

        let fragment_size = self.fuota_deployment.fragmentation_fragment_size as usize;
        let fragments =
            (self.fuota_deployment.payload.len() as f32 / fragment_size as f32).ceil() as usize;
        let padding =
            (fragment_size - (self.fuota_deployment.payload.len() % fragment_size)) % fragment_size;

        let fuota_devices = fuota::get_devices(self.job.fuota_deployment_id.into(), -1, 0).await?;

        // Filter on devices that have completed the previous step, but not yet the FragSessionSetup.
        let fuota_devices: Vec<fuota::FuotaDeploymentDevice> = fuota_devices
            .into_iter()
            .filter(|d| {
                d.mc_group_setup_completed_at.is_some()
                    && d.frag_session_setup_completed_at.is_none()
            })
            .collect();

        for fuota_dev in &fuota_devices {
            let pl = fragmentation::v1::Payload::FragSessionSetupReq(
                fragmentation::v1::FragSessionSetupReqPayload {
                    frag_session: fragmentation::v1::FragSessionSetuReqPayloadFragSession {
                        mc_group_bit_mask: [true, false, false, false],
                        frag_index: 0,
                    },
                    nb_frag: fragments as u16,
                    frag_size: fragment_size as u8,
                    padding: padding as u8,
                    control: fragmentation::v1::FragSessionSetuReqPayloadControl {
                        block_ack_delay: 0,
                        fragmentation_matrix: 0,
                    },
                    descriptor: [0, 0, 0, 0],
                },
            );

            device_queue::enqueue_item(device_queue::DeviceQueueItem {
                dev_eui: fuota_dev.dev_eui,
                f_port: self.device_profile.app_layer_params.ts004_f_port.into(),
                data: pl.to_vec()?,
                ..Default::default()
            })
            .await?;
        }

        if !fuota_devices.is_empty() {
            // There are devices pending setup, we need to re-run this job.
            self.job.scheduler_run_after =
                Utc::now() + TimeDelta::seconds(self.device_profile.uplink_interval as i64);
            Ok(Some(FuotaJob::FragSessionSetup))
        } else {
            // All devices have completed the setup, move on to next job.
            Ok(Some(FuotaJob::McSession))
        }
    }

    async fn multicast_session_setup(&mut self) -> Result<Option<FuotaJob>> {
        // Proceed with next step after reaching the max attempts.
        if self.job.attempt_count - 1 > self.job.max_retry_count {
            return Ok(Some(FuotaJob::Enqueue));
        }

        let fuota_devices = fuota::get_devices(self.job.fuota_deployment_id.into(), -1, 0).await?;

        // Filter on devices that have completed the previous step, but not yet the McSession.
        let fuota_devices: Vec<fuota::FuotaDeploymentDevice> = fuota_devices
            .into_iter()
            .filter(|d| {
                d.frag_session_setup_completed_at.is_some() && d.mc_session_completed_at.is_none()
            })
            .collect();

        for fuota_dev in &fuota_devices {
            // We want to start the session (retry_count + 1) x the uplink_interval.
            // Note that retry_count=0 means only one attempt.
            let session_start = (Utc::now()
                + TimeDelta::seconds(
                    (self.job.max_retry_count as i64 + 1)
                        * self.device_profile.uplink_interval as i64,
                ))
            .to_gps_time()
            .num_seconds()
                % (1 << 32);

            let pl = match self.fuota_deployment.multicast_group_type.as_ref() {
                "B" => multicastsetup::v1::Payload::McClassBSessionReq(
                    multicastsetup::v1::McClassBSessionReqPayload {
                        mc_group_id_header:
                            multicastsetup::v1::McClassBSessionReqPayloadMcGroupIdHeader {
                                mc_group_id: 0,
                            },
                        session_time: (session_start - (session_start % 128)) as u32,
                        time_out_periodicity:
                            multicastsetup::v1::McClassBSessionReqPayloadTimeOutPeriodicity {
                                time_out: self.fuota_deployment.multicast_timeout as u8,
                                periodicity: self.fuota_deployment.multicast_class_b_ping_slot_nb_k
                                    as u8,
                            },
                        dl_frequ: self.fuota_deployment.multicast_frequency as u32,
                        dr: self.fuota_deployment.multicast_dr as u8,
                    },
                ),
                "C" => multicastsetup::v1::Payload::McClassCSessionReq(
                    multicastsetup::v1::McClassCSessionReqPayload {
                        mc_group_id_header:
                            multicastsetup::v1::McClassCSessionReqPayloadMcGroupIdHeader {
                                mc_group_id: 0,
                            },
                        session_time: session_start as u32,
                        session_time_out:
                            multicastsetup::v1::McClassCSessionReqPayloadSessionTimeOut {
                                time_out: self.fuota_deployment.multicast_timeout as u8,
                            },
                        dl_frequ: self.fuota_deployment.multicast_frequency as u32,
                        dr: self.fuota_deployment.multicast_dr as u8,
                    },
                ),
                _ => {
                    return Err(anyhow!(
                        "Unsupported group-type: {}",
                        self.fuota_deployment.multicast_group_type
                    ))
                }
            };

            device_queue::enqueue_item(device_queue::DeviceQueueItem {
                dev_eui: fuota_dev.dev_eui,
                f_port: self.device_profile.app_layer_params.ts005_f_port.into(),
                data: pl.to_vec()?,
                ..Default::default()
            })
            .await?;
        }

        // In this case we need to exactly try the max. attempts, because this is what the
        // session-start time calculation is based on. If we continue with enqueueing too
        // early, the multicast-session hasn't started yet.
        self.job.scheduler_run_after =
            Utc::now() + TimeDelta::seconds(self.device_profile.uplink_interval as i64);
        Ok(Some(FuotaJob::McSession))
    }

    async fn enqueue(&mut self) -> Result<Option<FuotaJob>> {
        // Proceed with next step after reaching the max attempts.
        if self.job.attempt_count - 1 > self.job.max_retry_count {
            return Ok(Some(FuotaJob::FragStatus));
        }

        let payload_length = self.fuota_deployment.payload.len();
        let fragment_size = self.fuota_deployment.fragmentation_fragment_size as usize;
        let padding = (fragment_size - (payload_length % fragment_size)) % fragment_size;

        let fragments = (payload_length as f32 / fragment_size as f32).ceil() as usize;
        let redundancy = (fragments as f32
            * self.fuota_deployment.fragmentation_redundancy_percentage as f32
            / 100.0)
            .ceil() as usize;

        let mut payload = self.fuota_deployment.payload.clone();
        payload.extend_from_slice(&vec![0; padding]);

        let encoded_fragments = fragmentation::v1::encode(&payload, fragment_size, redundancy)?;

        for (i, frag) in encoded_fragments.iter().enumerate() {
            let pl =
                fragmentation::v1::Payload::DataFragment(fragmentation::v1::DataFragmentPayload {
                    index_and_n: fragmentation::v1::DataFragmentPayloadIndexAndN {
                        frag_index: 0,
                        n: (i + 1) as u16,
                    },
                    data: frag.clone(),
                });

            let _ = downlink::multicast::enqueue(multicast::MulticastGroupQueueItem {
                multicast_group_id: self.fuota_deployment.id,
                f_port: self.device_profile.app_layer_params.ts004_f_port as i16,
                data: pl.to_vec()?,
                ..Default::default()
            })
            .await?;
        }

        Ok(Some(FuotaJob::FragStatus))
    }

    async fn fragmentation_status(&mut self) -> Result<Option<FuotaJob>> {
        Ok(None)
    }
}
