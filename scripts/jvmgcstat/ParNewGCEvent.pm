#!/usr/bin/perl

package ParNewGCEvent;

use strict;
use warnings;

use Carp;                      #croak
use Scalar::Util qw(blessed);  #blessed, which gets the class name of an object;  

use MyUtils;

sub new
{
  my $className = shift @_;
  croak "ParNewGCEvent::new() is a class-function thus the 1st param should be the class name" if ref $className;

  my $self = {};

  $self->{timestamp}          = shift @_;

  $self->{YGOccupBeforeGC}    = MyUtils::toKB(shift @_);
  $self->{YGOccupAfterGC}     = MyUtils::toKB(shift @_);
  $self->{YGCapacity}         = MyUtils::toKB(shift @_);

  $self->{HeapOccupBeforeGC}  = MyUtils::toKB(shift @_);
  $self->{HeapOccupAfterGC}   = MyUtils::toKB(shift @_);
  $self->{HeapCapacity}       = MyUtils::toKB(shift @_);

  $self->{stw}                = (shift @_)*1000;

  bless($self, $className);

  return $self;
}

sub dump
{
  my $self = shift @_;
  croak "ParNewGCEvent::dump() is an object-function thus the 1st param should be the object" unless ref $self;

  my $seq = shift @_;
  my $withHeader = shift @_;
  my $file = shift @_;

  if(defined $file)
  {
    if ($withHeader==1)
    {
      print $file ("Seq,Timestamp,StopTheWorld(ms),YGCapacity(KB),YGOccupBeforeGC(KB),YGOccupAfterGC(KB),HeapCapacity(KB),HeapOccupBeforeGC(KB),HeapOccupAfterGC(KB)\n");
    }

    printf $file "%d,%0.3f,%0.4f,%d,%d,%d,%d,%d,%d\n", 
                 $seq, $self->{timestamp}, $self->{stw} , 
                 $self->{YGCapacity}, $self->{YGOccupBeforeGC}, $self->{YGOccupAfterGC},
                 $self->{HeapCapacity}, $self->{HeapOccupBeforeGC}, $self->{HeapOccupAfterGC};
  }
  else
  {
    if ($withHeader==1)
    {
      print "Seq\tTimestamp\tStopTheWorld(ms)\tYGCapacity(KB)\tYGOccupBeforeGC(KB)\tYGOccupAfterGC(KB)\tHeapCapacity(KB)\tHeapOccupBeforeGC(KB)\tHeapOccupAfterGC(KB)\n";
    }

    printf "%d\t%0.3f\t%0.4f\t%d\t%d\t%d\t%d\t%d\t%d\n",
                 $seq, $self->{timestamp}, $self->{stw} , 
                 $self->{YGCapacity}, $self->{YGOccupBeforeGC}, $self->{YGOccupAfterGC},
                 $self->{HeapCapacity}, $self->{HeapOccupBeforeGC}, $self->{HeapOccupAfterGC};
  }
}

1;
