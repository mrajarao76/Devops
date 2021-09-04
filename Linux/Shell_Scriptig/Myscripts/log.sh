#! /bin/bash
  
timestamp=$(date +%d_%m_%Y_%H_%M_%S)

echo " This is data to log file " >> ${timestamp}.log
echo " This is extra data to log file " >> ${timestamp}.log

date >> ${timestamp}.log
echo >> ${timestamp}.log

echo " Data written to log file successfully"