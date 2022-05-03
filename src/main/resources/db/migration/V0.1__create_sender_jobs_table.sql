CREATE TABLE sender_jobs
(
  job_id            VARCHAR(32),
  sender_id         INTEGER NOT NULL,
  status            VARCHAR(32),
  start_time        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
  end_time          TIMESTAMP(3) DEFAULT NULL,
  message           VARCHAR(512),
  error_message     VARCHAR(512),
  members_processed BIGINT,
  batch_to_process  BIGINT,
  batch_completed   BIGINT,
  last_update_time  TIMESTAMP(3) DEFAULT NULL,
  PRIMARY KEY (job_id)
);
