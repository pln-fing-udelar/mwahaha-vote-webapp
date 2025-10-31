CREATE DATABASE mwahaha;

USE mwahaha;

CREATE TABLE prompts
(
  prompt_id VARCHAR(10) NOT NULL,
  word1     VARCHAR(50),
  word2     VARCHAR(50),
  headline  VARCHAR(2048),
  url       VARCHAR(2048),
  prompt    VARCHAR(256),
  task      VARCHAR(5)  NOT NULL,
  PRIMARY KEY (prompt_id)
) ENGINE InnoDB;

CREATE TABLE systems
(
  system_id VARCHAR(100) NOT NULL,
  PRIMARY KEY (system_id)
) ENGINE InnoDB;

CREATE TABLE outputs
(
  prompt_id VARCHAR(10)   NOT NULL,
  system_id VARCHAR(100) NOT NULL,
  text      VARCHAR(2048) NOT NULL,
  PRIMARY KEY (prompt_id, system_id),
  INDEX (prompt_id),
  INDEX (system_id),
  FOREIGN KEY (prompt_id) REFERENCES prompts (prompt_id),
  FOREIGN KEY (system_id) REFERENCES systems (system_id)
) ENGINE InnoDB;

CREATE TABLE votes
(
  prompt_id      VARCHAR(10)  NOT NULL,
  system_id_a    VARCHAR(100) NOT NULL,
  system_id_b    VARCHAR(100) NOT NULL,
  session_id     CHAR(100)    NOT NULL,
  vote           CHAR(1)      NOT NULL,
  date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_offensive_a BOOL      DEFAULT 0,
  is_offensive_b BOOL      DEFAULT 0,
  PRIMARY KEY (prompt_id, system_id_a, system_id_b, session_id),
  INDEX (prompt_id, system_id_a, system_id_b),
  INDEX (prompt_id, system_id_a),
  INDEX (prompt_id, system_id_b),
  INDEX (prompt_id),
  INDEX (system_id_a),
  INDEX (system_id_b),
  INDEX (session_id),
  FOREIGN KEY (prompt_id, system_id_a) REFERENCES outputs (prompt_id, system_id),
  FOREIGN KEY (prompt_id, system_id_b) REFERENCES outputs (prompt_id, system_id)
) ENGINE InnoDB;
