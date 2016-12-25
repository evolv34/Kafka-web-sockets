!/bin/bash

if [ $(uname) != 'Linux' ]; then
	if [ $(docker-machine status default) != 'Stopped' ]; then
	
		docker rmi $(docker images -a --quiet --filter "dangling=true")
		docker rmi $(docker images -a | grep "^<none>" | awk "{print $3}")
	
		
		echo "building"
		./gradlew clean
		
		./gradlew jar
		
		docker build -t chanhub/kafka-realtime .
		
		#docker login
		
		#docker push chanhub/kafka-realtime
		
	else 
		echo "docker machine is not running"	
	fi
else
	docker rmi $(docker images -a --quiet --filter "dangling=true")
	docker rmi $(docker images -a | grep "^<none>" | awk "{print $3}")
		
	echo "building"
	./gradlew clean
	
	./gradlew jar
	
	docker build -t chanhub/kafka-realtime .
	
	#docker login
	
	#docker push chanhub/kafka-realtime
fi