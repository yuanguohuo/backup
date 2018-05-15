#!/usr/bin/perl

package MyUtils;

use strict;
use warnings;

use Carp;                      #croak
use Scalar::Util qw(blessed);  #blessed, which gets the class name of an object;  

sub toKB
{
  my $size = shift @_;
  if($size =~ /(\d+)([KMG])/)
  {
    if ($2 eq "K")
    {
      return $1;
    }

    if($2 eq "M")
    {
      return $1*1024;
    }

    if($2 eq "G")
    {
      return $1*1024*1024;
    }
  }
  else
  {
    croak "size $size is invalid";
  }
}

1;
