delimiter $$

CREATE TABLE `testing` (
  `jobId` bigint(20) NOT NULL AUTO_INCREMENT,
  `job` text CHARACTER SET latin1,
  `key_hash` varchar(45) CHARACTER SET latin1 DEFAULT NULL,
  `state` smallint(6) NOT NULL DEFAULT '0',
  `ttr` int(11) NOT NULL DEFAULT '0',
  `ts` double NOT NULL,
  PRIMARY KEY (`jobId`),
  KEY `id_state_trr_ts` (`jobId`,`state`,`ttr`,`ts`),
  KEY `id_key_ts` (`jobId`,`key_hash`,`ts`),
  KEY `key_ts` (`key_hash`,`ts`)
) ENGINE=InnoDB AUTO_INCREMENT=205 DEFAULT CHARSET=utf8$$

