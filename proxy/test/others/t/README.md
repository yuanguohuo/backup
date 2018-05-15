# install
 *    cpan Test::Nginx
    or openresty github source cpan ./

 -   install depends:
    1. using cpan 
      * cpan Digest::HMAC_SHA1
      * capn MIME::Base64
      * cpan File::Slurper
      * cpan Unicode:UTF8
      * perl -MCAPN -e 'perl-Digest-HMAC.noarch'
      * cpan  Digest::MD5

    2.  using anothe method
      * sudo yum -y install perl-Digest-HMAC

#   config

in the home dir ,create file  ~/.s3curl as below,only need change id and key as your required
```
%awsSecretAccessKeys = (
    # personal account
    personal => {
        id => 'O911PT5Z34WN8Q92C8YU',
        key => 'l0fjx5mHz3KoOOiiYILc6JtpXqVJNOCpYJXUlOIZ',
    },

);

```


# run test
##   1. prepare   
### path config
   we need config bashprofile when using the prove
   PATH=$PATH:$HOME/.local/bin:$HOME/bin:/usr/local/openresty-debug/luajit/bin/
    PATH=$PATH:/usr/local/openresty-debug/nginx/sbin:$PATH
    export PATH
    
### rgw config

   * in the storageproxy_conf.lua,change the isrgw var to true

### libradso config
   * in the storageproxy_conf.lua,change the isrgw var to !true
   * put ceph mon /etc/ceph to the test matheas
   * creat testpool

##   2.run method
   all on the test diretory
   -  run all tests on rgw:
      *  ./go bucketname objectname
      *  now the bucket create cannot be automic in the rgw ,so rgw based proxy cannot pass the s3-tests suite
         so we should using the librados based proxy to test by the s3-tests suite.
   -  run all tests on librados:
      *  ./gorados bucketname objectname
   -   run singlefile tests
      *  prove -v t/listbucket.t
     
# write test case
## examples
    using the t/001-delobj.t  as example for test proxy
  
# issue
    1. varilizaion config
    2. unit test
    3. interagrate the mulitpart
