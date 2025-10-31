#!/usr/bin/env bash
# Retrieve public and private IPs for all running instances and store them in hosts.txt
# Public IP section
aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" \
  --query 'Reservations[*].Instances[*].[PublicIpAddress,PrivateIpAddress,Tags[?Key==`Name`].Value | [0]]' \
  --output text | awk '
BEGIN { n=0; c_pub=""; }
$1 != "None" && $1 != "" {
  if ($3 == "client") {
    c_pub = $1
  } else {
    pub[n] = $1
    n++
  }
}
END {
  for (i=0; i<n; i++) printf("%s node%d\n", pub[i], i)
  if (c_pub != "") printf("%s client\n", c_pub)
}' > hosts.txt


# Private IP section
aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" \
  --query 'Reservations[*].Instances[*].[PublicIpAddress,PrivateIpAddress,Tags[?Key==`Name`].Value | [0]]' \
  --output text | awk '
BEGIN { n=0; c_pri=""; }
$2 != "None" && $2 != "" {
  if ($3 == "client") {
    c_pri = $2
  } else {
    pri[n] = $2
    n++
  }
}
END {
  for (i=0; i<n; i++) printf("%s node%d-private\n", pri[i], i)
  if (c_pri != "") printf("%s client-private\n", c_pri)
}' >> hosts.txt

echo "Generated hosts.txt:"
./generate-node-envs.sh
echo "Updated envs/"
cat hosts.txt
