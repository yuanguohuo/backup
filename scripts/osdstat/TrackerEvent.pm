#!/usr/bin/perl

package TrackerEvent;

use strict;
use warnings;
use Carp;                      #croak
use Scalar::Util qw(blessed);  #blessed, which gets the class name of an object;

sub new
{
  my $class = shift @_;
  croak "TrackerEvent::new() is a class-function" if ref $class;

  my $self = {@_};

  bless($self,$class);

  return $self;
}

sub name
{
  my $self = shift @_;
  croak "TrackerEvent::name() is an object-function" unless ref $self;

  my $param = shift @_;
  $self->{name} = $param if defined $param;
  
  $self->{name};
}

sub stamp
{
  my $self = shift @_;
  croak "TrackerEvent::stamp() is an object-function" unless ref $self;

  my $param = shift @_;
  $self->{stamp} = $param if defined $param;
  
  $self->{stamp};
}

sub stamp_micro_sec
{
  my $self = shift @_;
  croak "TrackerEvent::stamp_micro_sec() is an object-function" unless ref $self;

  my $param = shift @_;
  $self->{stamp_micro_sec} = $param if defined $param;
  
  $self->{stamp_micro_sec};
}

sub cost
{
  my $self = shift @_;
  croak "TrackerEvent::cost() is an object-function" unless ref $self;

  my $param = shift @_;
  $self->{cost} = $param if defined $param;
  
  $self->{cost};
}

sub to_str 
{
  my $self = shift @_;
  croak "TrackerEvent::to_str() is an object-function" unless ref $self;

  my $file = shift @_;

  if(defined $file)
  {
    print $file ($self->{stamp}, "\t", $self->{stamp_micro_sec}, "\t", $self->{cost}, "\t",  $self->{name}, "\n");
  }
  else
  {
    print $self->{stamp}, "\t", $self->{stamp_micro_sec}, "\t", $self->{cost}, "\t",  $self->{name}, "\n";
  }
}

1;
