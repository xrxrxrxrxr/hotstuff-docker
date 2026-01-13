# Implementation of our Isotaxis protocol
This repo is for the anonymous review of the paper Isotaxis: Optimal Asynchronous Byzantine Consensus with Ordering Linearizability.

## Environment
We use Rust v1.92.0 for the implementation and benchmarks.

## Test mode: 

- Global-scale test specifications can be found under ec2/ folder.
- The client calculates Latency, consensus nodes log TPS.

### Local test:
```
 ./run_test.sh
 docker-compose --profile "*" down    
```

### AWS EC2 test:
```
cd ec2
./start_instances.sh --plan plans/plan-*.json
NODE_IP_SOURCE=public ./get_ips.sh
```

`*` in command `./start_instances.sh --plan plan-*.json` is a configurable number for system scale. 
Alternatively, modify `plan-*.json` file under path `ec2/plans`

Note:  
Modify the key path to your own key path in the following files:
`ec2/deploy-ec2.sh`
`ec2/init-ec2.sh`
`ec2/upload_pubkey.sh`
