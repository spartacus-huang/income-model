

JAR_SOURCE="./target/scala-2.10/repayment_2.10-1.0.jar"
TARGET_HOST="root@121.201.7.180"
JAR_TARGET="${TARGET_HOST}:/usr/local/spark/"
SSH_FILE="/Users/welab/.ssh/kp-1r9z0fv2"

sbt compile 
sbt package

scp  -i "${SSH_FILE}" "${JAR_SOURCE}" "${JAR_TARGET}"
