#!/usr/bin/perl

package CMSGCInitMark;

use strict;
use warnings;

use Carp;                      #croak
use Scalar::Util qw(blessed);  #blessed, which gets the class name of an object;  

use MyUtils;

sub new
{
  my $className = shift @_;
  croak "CMSGCInitMark::new() is a class-function thus the 1st param should be the class name" if ref $className;

  my $self = {};

  $self->{timestamp}        = shift @_;

  $self->{TenuredOccup}     = MyUtils::toKB(shift @_);
  $self->{TenuredCapacity}  = MyUtils::toKB(shift @_);

  $self->{HeapOccup}        = MyUtils::toKB(shift @_);
  $self->{HeapCapacity}     = MyUtils::toKB(shift @_);

  $self->{stw}              = (shift @_)*1000;

  bless($self, $className);

  return $self;
}

sub dump
{
  my $self = shift @_;
  croak "CMSGCInitMark::dump() is an object-function thus the 1st param should be the object" unless ref $self;

  my $seq = shift @_;
  my $withHeader = shift @_;
  my $file = shift @_;

  if(defined $file)
  {
    if ($withHeader==1)
    {
      print $file ("Seq,Timestamp,StopTheWorld(ms),TenuredCapacity(KB),TenuredOccup(KB),HeapCapacity(KB),HeapOccup(KB)\n");
    }

    printf $file "%d,%0.3f,%0.4f,%d,%d,%d,%d\n", 
                 $seq, $self->{timestamp}, $self->{stw} , 
                 $self->{TenuredCapacity}, $self->{TenuredOccup},,
                 $self->{HeapCapacity}, $self->{HeapOccup};
  }
  else
  {
    if ($withHeader==1)
    {
      print ("Seq\tTimestamp\tStopTheWorld(ms)\tTenuredCapacity(KB)\tTenuredOccup(KB)\tHeapCapacity(KB)\tHeapOccup(KB)\n");
    }

    printf "%d\t%0.3f\t%0.4f\t%d\t%d\t%d\t%d\n",
                 $seq, $self->{timestamp}, $self->{stw} , 
                 $self->{TenuredCapacity}, $self->{TenuredOccup},,
                 $self->{HeapCapacity}, $self->{HeapOccup};
  }
}

1;
