#!/usr/bin/env perl
###############################################################################
# Spark log cleaner
# It removes some known and frequently occured lines from the spark log messages,
# so returns only the real information.
#
# usage:
# perl -w spark-log-cleaner.pl [spark log file]
# for example
# perl -w spark-log-cleaner.pl nohup.out
#
# Since I usually run Spark with nohup, the spark log file name is nohup.out
###############################################################################


my $in = $ARGV[0];
open(IN, $in) || die "Can't open file $in: $!\n";

while(<IN>) {
  if (
       $_ !~ m/INFO TaskSetManager: Starting task/
    && $_ !~ m/INFO Executor: Running task /
    && $_ !~ m/INFO TaskSetManager: Finished task /
    && $_ !~ m/INFO HadoopRDD: Input split: /
    && $_ !~ m/INFO FileOutputCommitter: Saved output of task /
    && $_ !~ m/INFO SparkHadoopMapRedUtil: /
    && $_ !~ m/INFO Executor: Finished task/
  ) {
    print $_;
  }
}
close IN;
