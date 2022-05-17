package config

import config.ConfigStore._
import pureconfig._
import io.prophecy.libs._
case class Config(fabricName: String, run_date: String) extends ConfigBase
