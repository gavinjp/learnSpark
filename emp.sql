/*
Navicat MySQL Data Transfer

Source Server         : 192.168.87.151
Source Server Version : 50714
Source Host           : localhost:3306
Source Database       : userdb

Target Server Type    : MYSQL
Target Server Version : 50714
File Encoding         : 65001

Date: 2017-07-30 20:42:55
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for emp
-- ----------------------------
DROP TABLE IF EXISTS `emp`;
CREATE TABLE `emp` (
  `id` int(11) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `deg` varchar(255) DEFAULT NULL,
  `salary` double DEFAULT NULL,
  `dept` varchar(255) DEFAULT NULL,
  `name2` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of emp
-- ----------------------------
INSERT INTO `emp` VALUES ('1201', 'gopal', 'manager', '50000', 'TP', 'aaa');
INSERT INTO `emp` VALUES ('1202', 'manisha', 'Proof reader', '50000', 'TP', 'bbb');
INSERT INTO `emp` VALUES ('1203', 'khalil', 'php dev', '30000', 'AC', 'kh');
INSERT INTO `emp` VALUES ('1204', 'prasanth', 'php dev', '30000', 'AC', 'pr');
INSERT INTO `emp` VALUES ('1205', 'kranthi', 'admin', '20000', 'TP', 'kr');
