build:
	rm -f list_buckets  put_obj upload_obj
	g++ -g -std=c++11 -laws-cpp-sdk-core -laws-cpp-sdk-s3 list_buckets.cpp -o list_buckets
	g++ -g -std=c++11 -laws-cpp-sdk-core -laws-cpp-sdk-s3 put_obj.cpp -o put_obj
	g++ -g -std=c++11 -laws-cpp-sdk-core -laws-cpp-sdk-s3 upload_obj.cpp -o upload_obj

run:
	export LD_LIBRARY_PATH=/usr/local/lib64; ./list_buckets
	export LD_LIBRARY_PATH=/usr/local/lib64; ./put_obj -a 127.0.0.1 -b testbuck10 -o ObjBBBB  -f ../../boost_1_59_0.tar.gz
	export LD_LIBRARY_PATH=/usr/local/lib64; ./upload_obj -a 127.0.0.1 -b testbuck10 -o xx/中文？￥测试.zip  -f ../../xx/中文？￥测试.zip
	export LD_LIBRARY_PATH=/usr/local/lib64; ./upload_obj -a 127.0.0.1 -b testbuck10 -o 中文？￥测试.zip   -f ../../xx/中文？￥测试.zip
