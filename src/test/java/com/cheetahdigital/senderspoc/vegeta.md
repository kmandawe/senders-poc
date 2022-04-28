# Vegeta Load Testing

`echo "GET http://localhost:8888/sendpipeline/23" | vegeta attack -workers=4 -max-workers=10 -duration=5s | tee results.bin | vegeta report`
