1. Description 
   This is an alternative way to deploy a ceph cluster as well as rados gateway. Although ceph-deploy is quite handy to do such work and it has even more power such as    installing ceph packages online, I like the way here I am introducing in several aspects:
       1. Deploy by one key press: all you need to do is writing the ceph.conf, after that you simply need to run a script. Nothing else!
       2. You have the ability to control every thing by writing ceph.conf: which daemon (monitor, osd or mds) runs on which host; configuration for all or a kind of daemons, or a specific daemon.
       3. Concurrent disk formatting: if you have 10 hosts, each has 12 disks, all hosts format their disks at the same time, thus the total time spending on disk formating is roughly 12 * {time for one disk format}, instead of 120 * {time for one disk format}.
       4. Persist mount in /etc/fstab in UUID style: thus, mount will be consistent after disk-unplug, disk-destroy and reboot. 


2. Prerequisites
   a. Install the ceph packages. It is not as convenient as ceph-deploy at this point, which can install ceph packages from internet. However, in production envrionment, we ofter compile ceph and install it, instead of installing online packages.
   b. configure SSH. You should make sure that the master host can ssh/scp with any host without the need for password (including the master itself), and any host (including the master itself) can ssh/scp with the master without the need for password.
   c. disable the firewall.


3. How to Use
   1. clone this project and run install.sh on every host; by default, it will be install to /usr/local/ceph
   2. pick one host as the master, any one. The master controls the deployment process;
   3. write the ceph.conf; there is an example /usr/local/ceph/deploy/ceph.conf, just back it up and modify it.
   3. run ./deploy.sh.
