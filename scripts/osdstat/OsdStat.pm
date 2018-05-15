#!/usr/bin/perl

package OsdStat;

use strict;
use warnings;
use Carp;                      #croak
use Scalar::Util qw(blessed);  #blessed, which gets the class name of an object;

use Date::Parse;

use TrackerEvent;
use TrackedOp;

sub new
{
  my $class = shift @_;
  croak "OsdStat::new() is a class-function" if ref $class;

  my $self = {};

  $self->{period} = 60;                         #report period 
  $self->{period_end_time} = 0;

  $self->{loglevel} = 8;
  $self->{logfile} = undef;

  $self->{outfile} = undef;

  $self->{pri_wr_threshold} = 200000;           #write timeout threshold, 200 milliseconds by default;
  $self->{pri_wr_ops_num} = 0;                  #number of write ops in current report period;
  $self->{pri_wr_timeout_num} = 0;              #number of timeout write ops in current report period;

  $self->{sub_wr_threshold} = 150000;           #write timeout threshold, 200 milliseconds by default;
  $self->{sub_wr_ops_num} = 0;                  #number of write ops in current report period;
  $self->{sub_wr_timeout_num} = 0;              #number of timeout write ops in current report period;


  $self->{rd_threshold} = 100000;               #read timeout threshold, 100 milliseconds by default;
  $self->{rd_ops_num} = 0;                      #number of read ops in current report period;
  $self->{rd_timeout_num} = 0;                  #number of timeout read ops in current report period;

  $self->{ot_threshold} = 150000;               #timeout threshold for ops that are not read or write (such as osd_sub_op_reply or ops finished with error);
  $self->{ot_ops_num} = 0;                      #number of ops that are not read or write in current period;
  $self->{ot_timeout_num} = 0;                  #number of timeout ops that are not read or write in current period;

  $self->{pd_ops} = {};                         #ops that are in progress; 

  $self->{hg_ops_num} = 0;                      #some ops hang up forever, record them when flush them out of memory;
  $self->{fl_threshold} = 120000000;            #flush the ops that are older than 120 seconds when ops_mem_max reached;

  $self->{ops_mem_num} = 0;                     #number of ops that are in memory;
  $self->{ops_mem_max} = 2048;                  #max number of ops that are in memory; some ops may hang up, we need to discard them if max number reached;  

  while(defined $_[0] && defined $_[1])
  {
    $self->{$_[0]} = $_[1];
    shift @_;
    shift @_;
  }

  my $i = 0;
  $self->{pri_wr_period_cost} = [];
  $self->{pri_wr_period_cost}->[$i++] = ["queued_for_pg",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["reached_pg",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["started",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["waiting for subops from",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["commit_queued_for_journal_write",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["write_thread_in_journal_buffer",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["journaled_completion_queued",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["sub_op_commit_rec from",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["op_commit",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["commit_sent",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["op_applied",0,0]; 
  $self->{pri_wr_period_cost}->[$i++] = ["done",0,0]; 
  $self->{pri_wr_size} = $i;

  $i = 0;
  $self->{sub_wr_period_cost} = [];
  $self->{sub_wr_period_cost}->[$i++] = ["queued_for_pg",0,0]; 
  $self->{sub_wr_period_cost}->[$i++] = ["reached_pg",0,0]; 
  $self->{sub_wr_period_cost}->[$i++] = ["started",0,0]; 
  $self->{sub_wr_period_cost}->[$i++] = ["commit_queued_for_journal_write",0,0]; 
  $self->{sub_wr_period_cost}->[$i++] = ["write_thread_in_journal_buffer",0,0]; 
  $self->{sub_wr_period_cost}->[$i++] = ["journaled_completion_queued",0,0]; 
  $self->{sub_wr_period_cost}->[$i++] = ["commit_sent",0,0]; 
  $self->{sub_wr_period_cost}->[$i++] = ["sub_op_applied",0,0]; 
  $self->{sub_wr_period_cost}->[$i++] = ["done",0,0]; 
  $self->{sub_wr_size} = $i;

  $i = 0;
  $self->{rd_period_cost} = [];
  $self->{rd_period_cost}->[$i++] = ["queued_for_pg",0,0];
  $self->{rd_period_cost}->[$i++] = ["reached_pg",0,0];
  $self->{rd_period_cost}->[$i++] = ["started",0,0];
  $self->{rd_period_cost}->[$i++] = ["done",0,0];
  $self->{rd_size} = $i;

  my $outf = $self->{outfile};
  croak "param outfile is mandatory for OsdStat::new()" unless defined $outf;

  print $outf ("Date,Time");

  for (my $i=0; $i < $self->{rd_size}; $i++)
  {
    print $outf (",","Read_".$self->{rd_period_cost}->[$i]->[0]);
  }

  for (my $i=0; $i < $self->{pri_wr_size}; $i++)
  {
    print $outf (",","pWrite_".$self->{pri_wr_period_cost}->[$i]->[0]);
  }

  for (my $i=0; $i < $self->{sub_wr_size}; $i++)
  {
    print $outf (",","sWrite_".$self->{sub_wr_period_cost}->[$i]->[0]);
  }
  print $outf ("\n");

  bless($self,$class);

  return $self;
}

sub log
{
  my $self = shift @_;
  croak "OsdStat::log() is an object-function" unless ref $self;

  my $level = shift @_;

  if($level <= $self->{loglevel})
  {
    if(defined $self->{logfile})
    {
      my $file = $self->{logfile};
      print $file (@_);
    }
    else
    {
      print STDOUT (@_);
    }
  }
}

sub reset
{
  my $self = shift @_;
  croak "OsdStat::reset() is an object-function" unless ref $self;

  $self->{pri_wr_ops_num} = 0;
  $self->{pri_wr_timeout_num} = 0;

  $self->{sub_wr_ops_num} = 0;
  $self->{sub_wr_timeout_num} = 0;

  $self->{rd_ops_num} = 0;
  $self->{rd_timeout_num} = 0;

  $self->{ot_ops_num} = 0;  
  $self->{ot_timeout_num} = 0;

  for(my $i=0; $i<$self->{pri_wr_size}; $i++)
  {
    $self->{pri_wr_period_cost}->[$i]->[1] = 0; 
    $self->{pri_wr_period_cost}->[$i]->[2] = 0; 
  }

  for(my $i=0; $i<$self->{sub_wr_size}; $i++)
  {
    $self->{sub_wr_period_cost}->[$i]->[1] = 0; 
    $self->{sub_wr_period_cost}->[$i]->[2] = 0; 
  }

  for(my $i=0; $i<$self->{rd_size}; $i++)
  {
    $self->{rd_period_cost}->[$i]->[1] = 0;
    $self->{rd_period_cost}->[$i]->[2] = 0;
  }
}

sub out_csv
{
  my $self = shift @_;
  croak "OsdStat::out_csv() is an object-function" unless ref $self;

  croak "param time is missing for OsdStat::out_csv()" unless defined $_[0];

  my ($sec,$min,$hour,$day,$mon,$year,$wday,$yday,$isdst)=localtime($_[0]);
  $mon++;

  my $outf = $self->{outfile};
  print $outf ($year."-".$mon."-".$day,",",$hour.":".$min.":".$sec);

  my $pri_wr = $self->{pri_wr_period_cost};
  my $sub_wr = $self->{sub_wr_period_cost};
  my $read = $self->{rd_period_cost};

  for(my $i=0; $i<$self->{rd_size}; $i++)
  {
    printf $outf (",%.2f", $read->[$i]->[1]==0 ? 0 : $read->[$i]->[2]/$read->[$i]->[1]);
  }

  for(my $i=0; $i<$self->{pri_wr_size}; $i++)
  {
    printf $outf (",%.2f", $pri_wr->[$i]->[1]==0 ? 0 : $pri_wr->[$i]->[2]/$pri_wr->[$i]->[1]);
  }

  for(my $i=0; $i<$self->{sub_wr_size}; $i++)
  {
    printf $outf (",%.2f", $sub_wr->[$i]->[1]==0 ? 0 : $sub_wr->[$i]->[2]/$sub_wr->[$i]->[1]);
  }
  print $outf ("\n");
}

sub dump
{
  my $self = shift @_;
  croak "OsdStat::dump() is an object-function" unless ref $self;

  my $lvl = shift @_;
  $lvl = 10 unless defined $lvl;

  $self->log($lvl,"============================\n");
  my $logf="undef";
  $logf=$self->{logfile} if defined $self->{logfile};
  $self->log($lvl,"period=",$self->period()," logfile=",$logf," loglevel=",$self->{loglevel},"\n");
  $self->log($lvl,"pri_wr_threshold=",$self->pri_wr_threshold()," pri_wr_ops_num=",$self->pri_wr_ops_num()," pri_wr_timeout_num=", $self->pri_wr_timeout_num(),"\n");
  $self->log($lvl,"sub_wr_threshold=",$self->sub_wr_threshold()," sub_wr_ops_num=",$self->sub_wr_ops_num()," sub_wr_timeout_num=", $self->sub_wr_timeout_num(),"\n");
  $self->log($lvl,"rd_threshold=",$self->rd_threshold()," rd_ops_num=",$self->rd_ops_num()," rd_timeout_num=", $self->rd_timeout_num(),"\n");
  $self->log($lvl,"ot_threshold=",$self->ot_threshold()," ot_ops_num=",$self->ot_ops_num()," ot_timeout_num=", $self->ot_timeout_num(),"\n");
  $self->log($lvl,"fl_threshold=",$self->{fl_threshold}," hg_ops_num=",$self->{hg_ops_num}," ops_mem_num=",$self->{ops_mem_num}," ops_mem_max=",$self->{ops_mem_max},"\n");

  $self->log($lvl,"--------pri write stat------\n");
  for (my $i=0; $i < $self->{pri_wr_size}; $i++)
  {
    my $count = $self->{pri_wr_period_cost}->[$i]->[1];
    my $cost =  $self->{pri_wr_period_cost}->[$i]->[2];
    my $avg = 0;
    $avg = $cost/$count if ($count>0);
    $self->log($lvl,$cost,"\t",$count,"\t",$avg,"\t",$self->{pri_wr_period_cost}->[$i]->[0],"\n");
  }

  $self->log($lvl,"--------sub write stat------\n");
  for (my $i=0; $i < $self->{sub_wr_size}; $i++)
  {
    my $count = $self->{sub_wr_period_cost}->[$i]->[1];
    my $cost =  $self->{sub_wr_period_cost}->[$i]->[2];
    my $avg = 0;
    $avg = $cost/$count if ($count>0);
    $self->log($lvl,$cost,"\t",$count,"\t",$avg,"\t",$self->{sub_wr_period_cost}->[$i]->[0],"\n");
  }

  $self->log($lvl,"----------read stat---------\n");
  for (my $i=0; $i < $self->{rd_size}; $i++)
  {
    my $count = $self->{rd_period_cost}->[$i]->[1];
    my $cost =  $self->{rd_period_cost}->[$i]->[2];
    my $avg = 0;
    $avg = $cost/$count if ($count>0);
    $self->log($lvl,$cost,"\t",$count,"\t",$avg,"\t",$self->{rd_period_cost}->[$i]->[0],"\n");
  }

  $self->log($lvl,"------------ops-------------\n");
  while((my $key, my $value) = each(%{$self->{pd_ops}}))
  {
    #$value->dump($self->{logfile});
  }

  $self->log($lvl,"\n\n");
}

sub add_evt
{
  my $self = shift @_;
  croak "OsdStat::add_evt() is an object-function" unless ref $self;

  my $seq = shift @_;
  croak "argument seq is not defined for OsdStat::add_evt()" unless defined $seq;
  
  my $evt_name = shift @_;
  croak "argument evt_name is not defined for OsdStat::add_evt()" unless defined $evt_name;

  my $evt_stamp = shift @_;
  croak "argument evt_stamp is not defined for OsdStat::add_evt()" unless defined $evt_stamp;

  my $op_type = shift @_;
  croak "argument op_type is not defined for OsdStat::add_evt()" unless defined $op_type;

  my $descrip = shift @_;
  croak "argument descrip is not defined for OsdStat::add_evt()" unless defined $descrip;

  #too slow! because it forks new process;
  #my $stamp_micro_sec=`date +%s%N -d "$evt_stamp"`;  # in nano seconds;
  #$stamp_micro_sec /= 1000;                          # to micro seconds; 

  my $sec = str2time(substr($evt_stamp,0,19));
  my $msec = substr($evt_stamp,20,6);
  my $stamp_micro_sec = $sec * 1000000  + $msec;
  
  print ("stamp: ", $evt_stamp, "\tsec-msec: ", $sec, $msec, "\tmicrosec: ", $stamp_micro_sec, "\n");

  my $evt = TrackerEvent->new(name=>$evt_name,stamp=>$evt_stamp,stamp_micro_sec=>$stamp_micro_sec);

  my $op = undef;
  if(defined $self->{pd_ops}->{$seq})
  {
    $op = $self->{pd_ops}->{$seq};
  }
  else
  {
    $self->{pd_ops}->{$seq} = TrackedOp->new($seq,$op_type,$descrip);
    $op = $self->{pd_ops}->{$seq};

    $self->{ops_mem_num}++;
    if($self->{ops_mem_num} > $self->{ops_mem_max})
    {
      print "there are ", $self->{ops_mem_num}, " ops in memory now\n";
      croak "too large memory consumed";
    }
  }

  if($op->io_type() =~ "unkn")
  {
    if($evt_name =~ "commit_queued_for_journal_write") # now we know it's a write op, move it to wr_ops;
    {
      $op->io_type("wr");
    }
    elsif($evt_name =~ "no_write_op")
    {
      $op->io_type("rd");
    }
  }

  $op->add_evt($evt);

  if($evt_name =~ "done")
  {
    if($op->io_type() =~ "wr")
    {
      if($op->op_type() =~ "osd_op")
      {
        $self->{pri_wr_ops_num}++;
        if($op->cost()>=$self->{pri_wr_threshold})
        {
          $self->{pri_wr_timeout_num}++;
          $op->dump($self->{logfile});
  
          for (my $i=0; $i<$op->num(); $i++)
          {
            my $e = $op->get_evt($i);

            my $evtName = $e->name();

            if($evtName =~ /waiting for subops from/)
            {
              $evtName = "waiting for subops from"
            }
            else
            {
              if($evtName =~ /sub_op_applied_rec from/)
              {
                $evtName = "sub_op_applied_rec from"
              }
              else
              {
                if($evtName =~ /sub_op_commit_rec from/)
                {
                  $evtName = "sub_op_commit_rec from"
                }
              }
            }

            for (my $i=0; $i < $self->{pri_wr_size}; $i++)
            {
              if($evtName eq $self->{pri_wr_period_cost}->[$i]->[0])
              {
                $self->{pri_wr_period_cost}->[$i]->[1]++;
                $self->{pri_wr_period_cost}->[$i]->[2] += $e->cost();
                last;
              }
            }
          }
        }
      }
      elsif($op->op_type() =~ "osd_repop")
      {
        $self->{sub_wr_ops_num}++;
        if($op->cost()>=$self->{sub_wr_threshold})
        {
          $self->{sub_wr_timeout_num}++;
          $op->dump($self->{logfile});

          for (my $i=0; $i<$op->num(); $i++)
          {
            my $e = $op->get_evt($i);

            my $evtName = $e->name();

            for (my $i=0; $i < $self->{sub_wr_size}; $i++)
            {
              if($evtName eq $self->{sub_wr_period_cost}->[$i]->[0])
              {
                $self->{sub_wr_period_cost}->[$i]->[1]++;
                $self->{sub_wr_period_cost}->[$i]->[2] += $e->cost();
                last;
              }
            }
          }
        }
      }
      else
      {
        croak "invalid op type ", $op->op_type(), " for wr op";
      }
    }
    elsif($op->io_type() =~ "rd")
    {
      $self->{rd_ops_num}++;
      if($op->cost()>=$self->{rd_threshold})
      {
        $self->{rd_timeout_num}++;
        $op->dump($self->{logfile});

        for (my $i=0; $i<$op->num(); $i++)
        {
          my $e = $op->get_evt($i);
          my $ecost = $e->cost();

          my $evtName = $e->name();

          for (my $i=0; $i < $self->{rd_size}; $i++)
          {
            if($evtName eq $self->{rd_period_cost}->[$i]->[0])
            {
              $self->{rd_period_cost}->[$i]->[1]++;
              $self->{rd_period_cost}->[$i]->[2] += $e->cost();
              last;
            }
          }
        }
      }
    }
    else
    {
      $self->{ot_ops_num}++;
      if($op->cost()>=$self->{ot_threshold})
      {
        $op->io_type("ot");
        $self->{ot_timeout_num}++;
        $op->dump($self->{logfile});
      }
    }

    delete $self->{pd_ops}->{$seq};
    $self->{ops_mem_num}--;
  }

  if( $self->{period_end_time} == 0 )  #meet only when the 1st time;
  {
    $self->{period_end_time} = $sec + $self->{period};
  }

  if( $sec >= $self->{period_end_time} )
  {
    $self->{period_end_time} = $sec + $self->{period};
    $self->dump(1);
    $self->out_csv($sec);
    $self->reset();
  }
}

sub period
{
  $_[0]->{period} = $_[1] if defined $_[1];
  $_[0]->{period};
}

sub pri_wr_threshold 
{
  $_[0]->{pri_wr_threshold} = $_[1] if defined $_[1];
  $_[0]->{pri_wr_threshold};
}

sub pri_wr_ops_num 
{
  $_[0]->{pri_wr_ops_num} = $_[1] if defined $_[1];
  $_[0]->{pri_wr_ops_num};
}

sub pri_wr_timeout_num 
{
  $_[0]->{pri_wr_timeout_num} = $_[1] if defined $_[1];
  $_[0]->{pri_wr_timeout_num};
}

sub sub_wr_threshold 
{
  $_[0]->{sub_wr_threshold} = $_[1] if defined $_[1];
  $_[0]->{sub_wr_threshold};
}

sub sub_wr_ops_num 
{
  $_[0]->{sub_wr_ops_num} = $_[1] if defined $_[1];
  $_[0]->{sub_wr_ops_num};
}

sub sub_wr_timeout_num 
{
  $_[0]->{sub_wr_timeout_num} = $_[1] if defined $_[1];
  $_[0]->{sub_wr_timeout_num};
}

sub rd_threshold 
{
  $_[0]->{rd_threshold} = $_[1] if defined $_[1];
  $_[0]->{rd_threshold};
}

sub rd_ops_num 
{
  $_[0]->{rd_ops_num} = $_[1] if defined $_[1];
  $_[0]->{rd_ops_num};
}

sub rd_timeout_num 
{
  $_[0]->{rd_timeout_num} = $_[1] if defined $_[1];
  $_[0]->{rd_timeout_num};
}

sub ot_threshold 
{
  $_[0]->{ot_threshold} = $_[1] if defined $_[1];
  $_[0]->{ot_threshold};
}

sub ot_ops_num 
{
  $_[0]->{ot_ops_num} = $_[1] if defined $_[1];
  $_[0]->{ot_ops_num};
}

sub ot_timeout_num 
{
  $_[0]->{ot_timeout_num} = $_[1] if defined $_[1];
  $_[0]->{ot_timeout_num};
}

1;
