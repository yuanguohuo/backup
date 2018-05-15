#!/usr/bin/perl

use strict;
use warnings;
use Carp;                      #croak

use File::Tail;
use Getopt::Std;

use OsdStat;
use TrackerEvent;
use TrackedOp;

sub usage()
{
  print "Usage: osd-stat.pl -o <osd> -p <period> -r <read-threshold> -w <pri-write-threshold> -s <sub-write-threshold> -h\n";
  print "       -o osd:                 the osd to monitor, like osd.x\n";
  print "       -p period:              period of time to report, in seconds\n";
  print "       -r read-threshold:      don't take read ops that takes less than this time (in micro-seconds) into account\n";
  print "       -w pri-write-threshold: don't take pri-write ops that takes less than this time (in micro-seconds) into account\n";
  print "       -s sub-write-threshold: don't take sub-write ops that takes less than this time (in micro-seconds) into account\n";
  print "       -h print this help message\n";
}

my %opts = ();
getopts('o:p:r:w:s:h',\%opts);

my $osd              = undef;
my $period           = 60;
my $read_threshold   = 100000;
my $pWrite_threshold = 200000;
my $sWrite_threshold = 150000;

foreach my $k (keys %opts)
{
  if($k eq "o")
  {
    $osd = $opts{$k};
  }
  elsif($k eq "p")
  {
    $period = $opts{$k};
  }
  elsif($k eq "r")
  {
    $read_threshold = $opts{$k};
  }
  elsif($k eq "w")
  {
    $pWrite_threshold = $opts{$k};
  }
  elsif($k eq "s")
  {
    $sWrite_threshold = $opts{$k};
  }
  elsif($k eq "h")
  {
    usage();
    exit 0;
  }
}

if (defined $osd)
{
  chomp $osd;
  unless ($osd =~ "osd.[0-9]+")
  {
    print "ERROR: param <osd> should be in osd.x format\n";
    usage(); 
    exit 1;
  }

  my $osd_id = `echo $osd | sed -e 's/osd.//'`;
  my $tmpout = `ps -ef | grep "ceph-osd -i $osd_id" | grep -v grep 2> /dev/null`;
  if( $tmpout eq "" )
  {
    print "ERROR: $osd is not running on this host\n";
    usage(); 
    exit 1;
  }
}
else
{
  my $osd_id = `ps -ef | grep -o "ceph-osd -i [0-9][0-9]*" | cut -d ' ' -f 3 | head -n 1`;
  chomp $osd_id;
  if( $osd_id eq "" )
  {
    print "ERROR: there seems no OSD running on this host\n";
    usage(); 
    exit 1;
  }
  $osd="osd.".$osd_id;
}

print $osd, "\n";
print $period, "\n";
print $read_threshold,"\n";
print $pWrite_threshold, "\n";
print $sWrite_threshold, "\n";

my $cmd="ceph daemon $osd config set debug_optracker 100 ; ceph daemon $osd config set debug_osd 100 ; ceph daemon $osd config set debug_filestore 100 ; ceph daemon $osd config set debug_journal 100 ; echo > /var/ceph/logs/$osd.log";
`$cmd`;

my $myname="huoyuanguo";
my $myhome="/home/".$myname;
my $hostname = `hostname`;
chomp($hostname);
my $timestamp = `date +%Y%m%d%H%M%S`;
chomp($timestamp);

my $osdlog=File::Tail->new(
            name=>"/var/ceph/logs/".$osd.".log",
            tail=>-1);

my $logfile=$myhome."/osdstat/log/timeout-ops_".$timestamp.".log";
my $outfile=$myhome."/osdstat/log/".$hostname."_".$timestamp."_osdstat.csv";

my $LOGFILE;
my $OUTFILE;

open($LOGFILE, ">", $logfile) or die "failed to open $logfile: $!\n";
open($OUTFILE, ">", $outfile) or die "failed to open $outfile: $!\n";

#disable the buffer for LOGFILE and OUTFILE
select $LOGFILE;
$| = 1;
select $OUTFILE;
$| = 1;
select STDOUT;

my $osd_stat = OsdStat->new("logfile",$LOGFILE,"loglevel",8,"outfile",$OUTFILE,"period",$period,"rd_threshold",$read_threshold,"pri_wr_threshold",$pWrite_threshold,"sub_wr_threshold",$sWrite_threshold);

while( defined(my $line=$osdlog->read))
{
  if($line =~ /-- op tracker -- seq: (\d+), time: ([-:\s\d\.]+), event: (.*), op: (osd_op|osd_repop)(.*)$/)
  {
    my $seq=$1;
    my $evt_stamp=$2;
    my $evt_name=$3;
    my $op_type=$4;
    my $descrip=$5;

    $osd_stat->add_evt($seq, $evt_name, $evt_stamp, $op_type, $descrip);
  }
}
