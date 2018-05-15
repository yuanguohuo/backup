#!/usr/bin/perl

use strict;
use warnings;
use Carp;   #croak

use Getopt::Std;

use ParNewGCEvent;
use CMSGCInitMark;
use CMSGCReMark;

sub usage()
{
  print "Usage: gcstat.pl -o <output-dir> -l <gc-log-file> -h\n";
  print "       -o output-dir:          the directory to store the generated csv files\n";
  print "       -l gc-log-file:         the jvm gc log file\n";
  print "       -h print this help message\n";
}

my %opts = ();
getopts('o:l:',\%opts);

my $gclog  = undef;
my $outdir = undef;

foreach my $k (keys %opts)
{
  if($k eq "o")
  {
    $outdir= $opts{$k};
  }
  elsif($k eq "l")
  {
    $gclog = $opts{$k};
  }
  elsif($k eq "h")
  {
    usage();
    exit 0;
  }
  else
  {
    usage();
    exit 1;
  }
}

if(not defined $gclog)
{
  print "Error: option -l <gc-log-file> is mandatory\n";
  usage();
  exit 1;
}

if(not defined $outdir)
{
  $outdir="./";
}

open(my $GC_LOG, "<", $gclog) or die "failed to open $gclog: $!\n";

my $hostname = `hostname`;
chomp($hostname);
my $currTime = `date +%Y%m%d%H%M%S`;
chomp($currTime);

my $parNewCsv="$outdir/$hostname-jvm-ParNewGC-$currTime.csv";
open(my $ParNewCSV, ">", $parNewCsv) or die "failed to open $parNewCsv: $!\n";

my $initMarkCsv="$outdir/$hostname-jvm-InitMark-$currTime.csv";
open(my $InitMarkCSV, ">", $initMarkCsv) or die "failed to open $initMarkCsv: $!\n";

my $reMarkCsv="$outdir/$hostname-jvm-ReMark-$currTime.csv";
open(my $ReMarkCSV, ">", $reMarkCsv) or die "failed to open $reMarkCsv: $!\n";


my $parNewSeq = 0;
my $initMarkSeq = 0;
my $reMarkSeq = 0;
while (my $line = <$GC_LOG>)
{
  #0.579: [GC (Allocation Failure) 0.579: [ParNew: 26240K->2734K(29504K), 0.0053728 secs] 26240K->2734K(521024K), 0.0054565 secs] [Times: user=0.01 sys=0.00, real=0.01 secs]
  #249608.840: [GC (CMS Final Remark) 264959.119: [GC (Allocation Failure) 264959.119: [ParNew: 420362K->951K(471872K), 0.0148455 secs] 1238781K->819428K(10433344K), 0.0149727 secs] [Times: user=0.20 sys=0.04, real=0.02 secs]
  #8603.744: [GC (GCLocker Initiated GC) 8603.744: [ParNew: 1415616K->157248K(1415616K), 0.1086919 secs] 6453925K->5929883K(10328512K), 0.1088375 secs] [Times: user=0.80 sys=0.26, real=0.11 secs]
  if($line =~ /([\d\.]+):.*\[ParNew: (\d+[KMG])->(\d+[KMG])\((\d+[KMG])\), [\d\.]+ secs\] (\d+[KMG])->(\d+[KMG])\((\d+[KMG])\), ([\d\.]+) secs\]/)
  {
    my $parNewEvt = ParNewGCEvent->new($1, $2, $3, $4, $5, $6, $7, $8);

    if($parNewSeq == 0)
    {
      $parNewEvt->dump($parNewSeq, 1, $ParNewCSV);
    }
    else
    {
      $parNewEvt->dump($parNewSeq, 0, $ParNewCSV);
    }
    $parNewSeq++;
  }
  #3.047: [GC (CMS Initial Mark) [1 CMS-initial-mark: 6109K(491520K)] 9891K(521024K), 0.0054504 secs] [Times: user=0.01 sys=0.01, real=0.00 secs]
  elsif ($line =~ /([\d\.]+): \[GC \(CMS Initial Mark\) \[1 CMS-initial-mark: (\d+[KMG])\((\d+[KMG])\)\] (\d+[KMG])\((\d+[KMG])\), ([\d\.]+) secs]/)
  {
    my $cmsGCinitMark = CMSGCInitMark->new($1, $2, $3, $4, $5, $6);

    if($initMarkSeq == 0)
    {
      $cmsGCinitMark->dump($initMarkSeq, 1, $InitMarkCSV);
    }
    else
    {
      $cmsGCinitMark->dump($initMarkSeq, 0, $InitMarkCSV);
    }

    $initMarkSeq++;
  }
  #3.079: [GC (CMS Final Remark) [YG occupancy: 4866 K (29504 K)]3.079: [Rescan (parallel) , 0.0083167 secs]3.088: [weak refs processing, 0.0000284 secs]3.088: [class unloading, 0.0032224 secs]3.091: [scrub symbol table, 0.0017638 secs]3.093: [scrub string table, 0.0005042 secs][1 CMS-remark: 6109K(491520K)] 10976K(521024K), 0.0143429 secs] [Times: user=0.03 sys=0.00, real=0.02 secs] 
  #                  $1
  elsif ($line =~ /([\d\.]+): \[GC \(CMS Final Remark\) \[YG occupancy: \d+ K \(\d+ K\)\][\d\.]+: \[Rescan \(parallel\) , [\d\.]+ secs\][\d\.]+: \[weak refs processing, [\d\.]+ secs\][\d\.]+: \[class unloading, [\d\.]+ secs\][\d\.]+: \[scrub symbol table, [\d\.]+ secs\][\d\.]+: \[scrub string table, [\d\.]+ secs\]\[1 CMS-remark: (\d+[KMG])\((\d+[KMG])\)\] (\d+[KMG])\((\d+[KMG])\), ([\d\.]+) secs\]/)
  #                                       $2          $3             $4          $5            $6
  {
    my $cmsGCRemark = CMSGCReMark->new($1, $2, $3, $4, $5, $6);

    if($reMarkSeq == 0)
    {
      $cmsGCRemark->dump($reMarkSeq, 1, $ReMarkCSV);
    }
    else
    {
      $cmsGCRemark->dump($reMarkSeq, 0, $ReMarkCSV);
    }

    $reMarkSeq++;
  }
}

close($GC_LOG);
close($ParNewCSV);
close($InitMarkCSV);
close($ReMarkCSV);
