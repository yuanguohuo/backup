[How To Build And Install]
    1. install dependencies, such as cmake3
    2. mkdir a dir at any place you want and cd to that dir,  such as:
          mkdir /tmp/build
          cd /tmp/build
    3. build and install like this
          cmake3 -DBUILD_ONLY="s3"  {path/to/aws-sdk-cpp}
          make
          make install

    If it went well, the sdk (header files and libs) has been installed into
    /usr/local/include and /usr/local/lib64.
  
[How To Use it]
    See dir examples
