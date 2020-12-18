# kafka-test
Test to generate random temperatures and push them to a kafka stream.

Mostly taken from: https://github.com/segmentio/kafka-go

Run with: ```docker-compose up```

##Structure

One executable is used to run both the producer and consumer via CLI arguments. Each executable is run within its own container.

###Producer
```./go_build_linux_linx producer```

###Consumer
```./go_build_linux_linx consumer```

##TODO
- Better error handling
- Better godoc
