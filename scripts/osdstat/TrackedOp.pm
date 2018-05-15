#!/usr/bin/perl

package TrackedOp;

use strict; 
use warnings;
use Carp;                      #croak
use Scalar::Util qw(blessed);  #blessed, which gets the class name of an object;

use TrackerEvent;

sub new
{
  my $class = shift @_;
  croak "TrackedOp::new() is a class-function" if ref $class;

  my $seq = shift @_;
  croak "seq is mandatory for TrackedOp::new()" unless defined $seq;

  my $op_type = shift @_;
  croak "op_type is mandatory for TrackedOp::new()" unless defined $op_type;

  my $descrip = shift @_;

  my $self = {};
  $self->{seq} = $seq;
  $self->{evts} = [];           # an array of TrackerEvent references;
  $self->{num} = 0;             # num of TrackerEvents;
  $self->{cost} = 0;            # total cost of this TrackedOp;
  $self->{io_type} = "unkn";    # read or write; we don't know the type when construct it, and we'll set it to "write" when we find it's a write op;
  $self->{op_type} = $op_type;  # osd_op or osd_sub_op
  $self->{descrip} = $descrip;

  bless($self,$class);

  return $self;
}

#getter only
sub seq
{
  my $self = shift @_;
  croak "TrackedOp::seq() is an object-function" unless ref $self;

  croak "seq is not defined for TrackedOp object" unless defined $self->{seq};
  $self->{seq};
}

#getter only
sub num 
{
  my $self = shift @_;
  croak "TrackedOp::num() is an object-function" unless ref $self;

  croak "num is not defined for TrackedOp object" unless defined $self->{num};
  $self->{num};
}

#getter only
sub cost 
{
  my $self = shift @_;
  croak "TrackedOp::cost() is an object-function" unless ref $self;

  croak "cost is not defined for TrackedOp object" unless defined $self->{cost};
  $self->{cost};
}

#getter and setter
sub io_type 
{
  my $self = shift @_;
  croak "TrackedOp::io_type() is an object-function" unless ref $self;
  
  my $io_t = shift @_;

  $self->{io_type} = $io_t if defined $io_t;

  $self->{io_type};
}

#getter only
sub op_type 
{
  my $self = shift @_;
  croak "TrackedOp::op_type() is an object-function" unless ref $self;

  croak "op_type is not defined for TrackedOp object" unless defined $self->{op_type};
  $self->{op_type};
}

#getter only
sub descrip
{
  my $self = shift @_;
  croak "TrackedOp::descrip() is an object-function" unless ref $self;

  return "" unless defined $self->{descrip};
  $self->{descrip};
}

sub add_evt
{
  my $self = shift @_;
  croak "TrackedOp::add_evt() is an object-function" unless ref $self;

  my $evt = shift @_;   #a ref to TrackerEvent
  croak "argument evt is not defined for TrackedOp::add_evt()" unless defined $evt;
  croak "argument evt is not a TrackerEvent object" unless (blessed($evt) eq 'TrackerEvent');

  my $i=$self->{num}-1;
  while( $i>=0 && $self->{evts}->[$i]->stamp_micro_sec() > $evt->stamp_micro_sec() )
  {
    $self->{evts}->[$i+1] = $self->{evts}->[$i];
    $i--;
  }
  $i++;
  $self->{evts}->[$i] = $evt;
  $self->{num}++;

  my $cost=0;
  if($i==0)
  {
    $evt->cost(0);
  }
  elsif($i>0)
  {
    $cost = $evt->stamp_micro_sec() - $self->{evts}->[$i-1]->stamp_micro_sec();
    $evt->cost($cost);
  }

  if($i+1<$self->{num})
  {
    $cost = $self->{evts}->[$i+1]->stamp_micro_sec() - $self->{evts}->[$i]->stamp_micro_sec();
    $self->{evts}->[$i+1]->cost($cost);
  }

  $i = $self->{num}-1;
  $self->{cost} = $self->{evts}->[$i]->stamp_micro_sec() - $self->{evts}->[0]->stamp_micro_sec();
}

#getter only
sub get_evt
{
  my $self = shift @_;
  croak "TrackedOp::get_evt() is an object-function" unless ref $self;

  my $idx = shift @_;
  $idx = $self->{num}-1 unless defined $idx;

  $self->{evts}->[$idx];
}

sub evts()
{
  my $self = shift @_;
  croak "TrackedOp::evts() is an object-function" unless ref $self;
  $self->{evts};
}

sub dump
{
  my $self = shift @_;
  croak "TrackedOp::dump() is an object-function" unless ref $self;

  my $file = shift @_;

  if(defined $file)
  {
    print $file ($self->seq(), "\t", $self->cost(), "\t" , $self->io_type(), "\t", $self->op_type(), "\t", $self->descrip(), "\n");
  }
  else
  {
    print ($self->seq(), "\t", $self->cost(), "\t" , $self->io_type(), "\t", $self->op_type(), "\t", $self->descrip(), "\n");
  }

  for(my $i=0; $i<$self->{num}; $i++)
  {
    if(defined $file)
    {
      $self->{evts}->[$i]->to_str($file);
    }
    else
    {
      $self->{evts}->[$i]->to_str();
    }
  }
}

1;
