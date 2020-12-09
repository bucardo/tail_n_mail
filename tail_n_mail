#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil; cperl-indent-level: 4 -*-

## Tail one or more files, mail the new stuff to one or more emails
## Created at End Point Corporation by:
## Greg Sabino Mullane <greg@turnstep.com>
## Selena Deckelmann <selena@chesnok.com>
## See more contributors in the 'Changes' file
## BSD licensed
## For full documentation, please see: https://bucardo.org/tail_n_mail/

##
## Quick usage:
## Run: tail tail_n_mail > tail_n_mail.config
## Edit tail_n_mail.config in your favorite editor
## Run: perl tail_n_mail tail_n_mail.config
## Once working, put the above into a cron job

use strict;
use warnings;
use Data::Dumper          qw( Dumper              );
$Data::Dumper::Sortkeys = 1;
use Getopt::Long          qw( GetOptions          );
use File::Temp            qw( tempfile            );
use File::Basename        qw( dirname             );
use File::Spec::Functions qw( catfile             );
use POSIX                 qw( strftime localeconv );
use 5.14.0;

our $VERSION = '3.2.0';
my $app_string = "/* tail_n_mail version $VERSION */";

## Default message subject if not set elsewhere. Keywords replaced: FILE HOST NUMBER UNIQUE
my $DEFAULT_SUBJECT= 'Results for FILE on host: HOST UNIQUE : NUMBER';

## Set defaults for all the options, then read them in from command line
my %arg = (
## Show a help screen
              help             => 0,
## Show version and exit
              version          => 0,
## Do everything except send mail and update files
              dryrun           => 0,
## Verbose mode
              verbose          => 0,
## Be as quiet as possible
              quiet            => 0,
## Heavy debugging output
              debug            => 0,
## How long does psql keep trying to connect (in seconds)
              connect_timeout  => 15,
## Stop reading log files after this many bytes. For debugging only!
              debug_partial_read => 0,
## Stop reading log files after this many matches. For debugging only!
              debug_partial_matches => 0,
## Use the old rotation file
              debug_force_rotated => 0,
## Reset the marker of the log file to the current position
              reset            => 0,
## Rewind to a certain position inside the log file
              rewind           => 0,
## Which mail mode to use: can be sendmail or smtp
              mailmode         => 'sendmail',
## Use SMTP mode (sets mailmode to smtp)
              smtp             => 0,
## Location of the sendmail program. Expects to be able to use a -f argument.
              mailcom          => '/usr/sbin/sendmail',
## Mail options when using SMTP mode:
              mailserver       => 'example.com',
              mailuser         => 'example',
              mailpass         => 'example',
              mailport         => 465,
## Send mail as text/html and wrap in PRE for nicer alignment?
              html             => 0,
## Maximum size before we start wrapping lines
              wraplimit        => 990,
## Are we parsing Postgres logs?
              pgmode           => 1,
## What type of Postgres log we are parsing. Can be log, csv, pgbouncer, syslog, or syslog2
              pglog            => 'pg',
## What is Postgres's log_line_prefix
              log_line_prefix  => '',
## Alternate location for the service.conf file
              service_conf_file => '',
## Alternate location for the pgpass file
              pgpass_file       => '',
## Maximum number of bytes to pull back from pg_read_file
              pg_read_file_limit => 1_000_000,
## Maximum size of a statement before we truncate it.
              statement_size   => 1000,
## Maximum size of a nonparsed line (e.g. OS error) before we truncate it.
              nonparsed_statement_size   => 400,
## How to sort the output. Options are 'count' and 'date'
              sortby           => 'count',
## What type of file is this? Options are 'normal', 'duration', and 'tempfile'
              type             => 'normal',
## Allow the offset to be changed on the fly
              offset           => -1,
## Set the minimum duration
              duration         => -1,
## Set the minimum temp file size
              tempfile         => -1,
## Allow override of the log file to check
              file             => [],
## Strip out SQLSTATE codes from the FATAL and ERROR messages
              sqlstate         => 0,
## Do not send email
              nomail           => 0,
## Flatten similar queries into a canonical form
              flatten          => 1,
## Force flattening of Postgres items even if not in pgmode
              pgflatten        => 0,
## Hide the flattened version if there is only a single entry
              hideflatten      => 1,
## Do not show the STATEMENT for 'conflict with recovery' errors
              hide_conflict_error_details => 0,
## Move around in time for debugging
              timewarp         => 0,
## Show which line number problems are found on. Should be disabled for large/frequent reports
              find_line_number => 1,
## Show the number of errors per file
              show_file_numbers => 1,
## Show a simply counter before each error
              show_file_counter => 1,
## The maximum bytes we will go back and check per file
              maxsize          => 80_000_000,
## Only show X number of matches
              showonly         => 0,
## Send an email even if 0 matches were found
              mailzero         => 0,
## Append a signature to the end of mailed messages
              mailsig          => [],
## Perform some final prettification of queries
              pretty_query     => 1,
## Handling of canceled autovacuum entries. 0=normal, 1=summary at end, 2=ignore completely
              canceled_autovac => 1,
## Set the minimum number of matching duration entries that we care about
              duration_limit   => -30,
## Set the minimum number of matching tempfile entries that we care about
              tempfile_limit   => 0,
## The thousands separator for formatting numbers.
              tsep             => undef,
## Whether to turn off thousands separator in subject lines (mailman bug workaround)
              tsepnosub        => 0,
## The maximum file size before we split things up before mailing
              maxemailsize     => 10_000_000, ## bytes
## Do we skip lines that we cannot parse (e.g. rsync errors)?
              skip_non_parsed => 0,
## Do not generate a report if the only errors are from a known failing host. Can be 'all'
              skip_report_failing_host => '',
## Number of found errors resulting in a Nagios "CRITICAL" exit status
              critical => 0,
## Number of found errors resulting in a Nagios "WARNING" exit status
              warning => 0,
## Skip FILE entries from the config file unless they match this regex
              yesfile => [],
## Skip FILE entries from the config file if they match this regex
              nofile => [],
);

## Quick check for help items
for my $item (@ARGV) {
    if ($item =~ /^-+\?$/) {
        help();
    }
    ## Allow dashes or underscores in our options
    $item =~ s{^\-\-([\w_-]+)}{($a=$1) =~ s/[_-]/_/g; "--$a"}e;
}

GetOptions
 (
     \%arg,
   'verbose',
   'quiet',
   'debug',
   'debug_partial_read|debug-partial-read=i',
   'debug_partial_matches|debug-partial-matches=i',
   'debug_force_rotated|debug-force-rotated',
   'dryrun|dry-run|dry_run',
   'connect_timeout=i',
   'help',
   'nomail',
   'reset',
   'rewind=i',
   'version',
   'offset=i',
   'start_time=s',
   'duration=i',
   'tempfile=i',
   'find_line_number=i',
   'show_file_numbers=i',
   'show_file_counter=i',
   'file=s@',
   'skipfilebyregex=s@',
   'sqlstate',
   'type=s',
   'flatten!',
   'pgflatten',
   'hideflatten!',
   'hide_conflict_error_details!',
   'timewarp=i',
   'pgmode=s',
   'pglog=s',
   'log_line_prefix|log-line-prefix=s',
   'log_line_prefix_regex|log-line-prefix-regex=s@',
   'service_conf_file=s',
   'pgpass_file=s',
   'pg_read_file_limit=i',
   'maxsize=i',
   'sortby=s',
   'showonly=i',
   'mailmode=s',
   'smtp',
   'mailcom=s',
   'mailserver=s',
   'mailuser=s',
   'mailpass=s',
   'mailport=s',
   'mailzero',
   'mailsig=s@',
   'html',
   'tsep=s',
   'tsepnosub',
   'nolastfile',
   'pretty_query|pretty-query',
   'canceled_autovac|canceled-autovac=i',
   'duration_limit|duration-limit=i',
   'tempfile_limit|tempfile-limit=i',
   'statement_size|statement-size=i',
   'nonparsed_statement_size|nonparsed-statement-size=i',
   'tailnmailrc=s',
   'no-tailnmailrc|no_tailnmailrc',
   'maxemailsize=i',
   'skip_non_parsed|skip-non-parsed',
   'skip_report_failing_host=s',
   'force-letter-output',
   'warning=i',
   'critical=i',
   'yesfile=s@',
   'nofile=s@',
  ) or help();
++$arg{verbose} if $arg{debug};

if ($arg{version}) {
    print "$0 version $VERSION\n";
    exit 0;
}

sub help {
    print "Usage: $0 configfile [options]\n";
    print "For full documentation, please visit:\n";
    print "https://bucardo.org/tail_n_mail/\n";
    exit 0;
}
$arg{help} and help();

## First option is always the config file, which must exist.
my $configfile = shift or die qq{Usage: $0 configfile\n};
my $statefile = "$configfile.state";

## If the file has the name 'duration' in it, or we have set the duration parameter,
## switch to that type as the default
if ($configfile =~ /duration/i or $arg{duration} > 0) {
    $arg{type} = 'duration';
}

## If the file has the name 'tempfile' in it, or we have set the tempfile parameter,
## switch to that type as the default
if ($configfile =~ /tempfile/i or $arg{tempfile} > 0) {
    $arg{type} = 'tempfile';
}

## If the file has the name 'bouncer' in it, switch to pgbouncer mode
if ($configfile =~ /bouncer/i) {
    $arg{pglog} = 'pgbouncer';
}

## If the file has the name 'dryrun' in it, force dryrun mode
if ($configfile =~ /dryrun/) {
    $arg{dryrun} = 1;
}

## Quick expansion of leading tildes for the file argument
for (@{ $arg{file} }) {
    s{^~/?}{$ENV{HOME}/};
}

## The start_time must be in a specific format for now
if ($arg{start_time}) {
    $arg{original_start_time} = $arg{start_time}; ## For summary display
    $arg{start_time} =~ s/[^0-9]//g;
    14 == length $arg{start_time} or die "Argument start_time must be in the format YYYY-MM-DD HH:MI:SS\n";
}

## Save away our hostname
my $hostname = qx{hostname};
chomp $hostname;

## Global option variables
my %opt = (
    no_inherit => '',
);
my %itemcomment;

## Read in any rc files
parse_rc_files();
## Read in and parse the config file
parse_config_file();
## Read in any inherited config files and merge their information in
parse_inherited_files();
## Read in the state file and merge it in
parse_state_file();

## Quick check for duplicated FILE entries
my %dupefile;
$dupefile{ $_->{original} }++ for @{ $opt{file} };
if (grep { $_ > 1 } values %dupefile) {
    warn "Cannot continue, as the following FILE entries are duplicated:\n";
    for my $dupe (grep { $dupefile{$_} > 1 } sort keys %dupefile) {
        warn "  $dupe\n";
    }
    exit 1;
}

## Set our timeout
$ENV{PGCONNECT_TIMEOUT} = $arg{connect_timeout};

## Allow whitespace in configurable items by wrapping in double quotes
## Here, we remove them
for my $k (keys %opt) {
    $opt{$k} =~ s/^"(.+)"$/$1/;
}

my $pglog = $opt{pglog} || $arg{pglog};
my $pgmode = $opt{pgmode} // $arg{pgmode};

## Figure out each log_line_prefix, then create regexes for it
my $default_llp = ($pglog =~ /syslog/ ? '' : $pglog eq 'pgbouncer' ? '%tb %p ' : '%t [%p]');

my $llprefix = $opt{log_line_prefix} || $arg{log_line_prefix} || {};

$llprefix->{0} ||= $default_llp;

my $timezone_regex = '(?:[A-Z]{3,5}|[+-][0-9][0-9:]*)';

my ($llp_num, $llp1, %pgpidre);
while (($llp_num,$llp1) = each %$llprefix) {

  ## Remove any quotes around it
  $llp1 =~ s{^'(.+)'$}{$1};
  $llp1 =~ s{^"(.+)"$}{$1};

  ## Escape certain things that may confuse the regex parser
  $llp1 =~ s/([\-\[\]\(\)])/\\$1/g;

  ## Future use:
  my $llp2 = $llp1;
  my $llp3 = $llp1;

  ## Process the log_line_prefix and change it into a regex
  my ($havetime,$havepid) = (0,0);

  ## Some non-Postgres timestamps
  ## Example: Sun Oct 29 08:50:53.691196 2017
  $llp1 =~ s/%ts/(\\w\\w\\w \\w\\w\\w\\s+[0-9]+ [0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+ [0-9][0-9][0-9][0-9])/ and $havetime=1;

  ## For pgbouncer logs
  $llp1 =~ s/%tb/([0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.[0-9]+)/ and $havetime=1;

  ## This assumes timestamp comes before the pid!
  $llp1 =~ s/%t/([0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] $timezone_regex)/ and $havetime=1;
  $llp1 =~ s/%m/([0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+ $timezone_regex)/ and $havetime=1;
  if (!$havetime and $pglog !~ /syslog/) {
    $llp1 = "()$llp1";
  }
  $llp1 =~ s/%p/([0-9]+)/ and $havepid = 1;
  if ($havepid) {
      ## We want to capture only one of %p or %c
      $llp1 =~ s/%c/(?:\\S+)/;
  }
  else {
      $llp1 =~ s/%c/(\\S+)/ and $havepid = 1;
  }
  if (!$havepid and $pglog !~ /syslog/) {
    $llp1 = "()$llp1";
  }
  $llp1 =~ s/%l/[0-9]+/;
  $llp1 =~ s/%u/[\\[\\w\\-\\.\\]]*/;
  $llp1 =~ s/%d/[\\[\\w\\-\\.\\]]*/;
  $llp1 =~ s/%r/\\S*/;
  $llp1 =~ s/%h/\\S*/;
  $llp1 =~ s/%e/[0-9A-Z]{5}/;
  $llp1 =~ s/%q//;

  ## The application_name is quite a pain, because it often has whitespace inside of it.
  ## There is no way to easily tell where the app name ends and the next item begins.
  ## For example, your application_name is "foo bar" and the log_line_prefix has %a %d
  ## Does "foo bar baz" indicate an application_name of 'foo' and a db of 'bar',
  ## or does it indicate an application_name of 'foo bar' and a db of 'baz'?
  ## One workaround is to assume that a non-greedy regex can be used as long as there
  ## is some constant string after the application_name in the log_line_prefix.
  ## This is actually a fairly safe assumption, as it is not common to have completely
  ## bare escapes at the end of the log_line_prefix
  $llp1 =~ s/%a([^%])/.*?$1/;
  ## Fall back to the older version:
  $llp1 =~ s/%a/\\S*/;

  if ($pglog =~ /syslog/) {
    ## Syslog is a little more specific
    ## It's not standard, but usually standard 'enough' to build a working regex
    ## Add in timestamp, host, process name, pid, and number
    ## This will probably break if your log_line_prefix has a timestamp,
    ## but why would you do that if using syslog? :)
    $llp1 = $pglog eq 'syslog'
      ? "(.+?[0-9]) \\S+ \\S+\\[([0-9]+)\\]: \\[[0-9]+\\-[0-9]+\\] $llp1"
        : "(.+?[0-9]) \\S+ \\S+\\[([0-9]+)\\]: \\[[0-9]+\\] $llp1";
    $havetime = $havepid = 1;
  }

  $pgpidre{$llp_num}{1} = qr{^($llp1)(.*)}a;
  $arg{verbose} and $pgmode and warn "  Log line prefix regex $llp_num/1: $pgpidre{$llp_num}{1}\n";

  ## And a separate one for cluster-wide notices

  ## Items set clusterwide: %t %m %p
  $llp2 =~ s/%t/[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] $timezone_regex/;
  $llp2 =~ s/%m/[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+ $timezone_regex/;
  $llp2 =~ s/%p/[0-9]+/;
  $llp2 =~ s/%l/[0-9]+/;
  $llp2 =~ s/%q.*//;
  ## Items not set clusterwide: %u %d %r %h %i %c %s %v %x
  for my $char (qw/ u d r h i cs v x /) {
    $llp2 =~ s/%$char//g;
  }

  $pgpidre{$llp_num}{2} = qr{^$llp2}a;
  $arg{verbose} and $pgmode and warn "  Log line prefix regex $llp_num/2: $pgpidre{$llp_num}{2}\n";

  ## And one more for things that throw out everything except the timestamp
  ## May not work on against all log_line_prefixes

  ## If there is a %q, simply strip everything after it
  ## Otherwise, strip everything past the first escape
  $llp3 =~ s/\%q.*// or $llp3 =~ s/(.*?%\w).+/$1/;

  ## Convert %t and %m and %l and %p
  $llp3 =~ s/%t/[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] $timezone_regex/;
  $llp3 =~ s/%m/[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+ $timezone_regex/;
  $llp3 =~ s/%l/[0-9]+/;
  $llp3 =~ s/%p/[0-9]+/;

  ## Remove any other escapes
  $llp3 =~ s/%\w//g;

  ## We do a \s* later on, so keep this short
  $llp3 =~ s/\s$//;

  $pgpidre{$llp_num}{3} = qr{^$llp3}a;
  $arg{verbose} and $pgmode and warn "  Log line prefix regex $llp_num/3: $pgpidre{$llp_num}{2}\n";

}

## Keep track of changes to know if we need to rewrite the config file or not
my $changes = 0;

## Global regexes: may change per file
my ($exclude, $include, $exclude_prefix, $exclude_non_parsed, $bonus_include);

## Service names can have 'words' and dashes
my $servicenameregex = qr{\w[\w\-]*};

## Note if we bumped into maxsize when trying to read a file
my (%toolarge);

# Actual matching strings are stored here
my %find;

## Keep track of which entries are similar to the ones we've seen before for possible flattening
my %similar;

## Things that do not fit smoothly into the above categories
my %fancyoutput;

## Map filenames to "A", "B", etc. for clean output of multiple matches
my %fab;

## Total matches across all files
$opt{grand_total} = 0;

## For help in sorting later on
my (%fileorder, $filenum);

## Generic globals
my ($string,$time,$csv);

## If they requested no lastfile, remove it now
if ($arg{nolastfile}) {
    delete $opt{lastfile};
}

## If they want a mail signature, open the file(s) and read it in now
if (defined $arg{mailsig}->[0]) {
    ## Combine all files in order into a single string
    my $sigstring = '';
    my $fh;
    for my $sigfile (@{$arg{mailsig}}) {
        $sigfile =~ s{^~/?}{$ENV{HOME}/};
        if (! open $fh, '<', $sigfile) {
            warn qq{Could not open signature file "$sigfile": $!\n};
            exit 1;
        }
        { local $/; $sigstring .= <$fh>; }
        close $fh;
    }
    $arg{mailsignature} = $sigstring;
}

## Parse each file returned by pick_log_file until we start looping
my $last_logfile = '';
my @files_parsed;
my @problem_files;
my $filenumber = 0;
my $fileinfo = $opt{file}[$filenumber];
my $found_errors = 0;
my %badservice;
NEXTLOG: {

    ## Generate the next log file to parse
    my $logfile = pick_log_file($fileinfo);

    ## If undefined or same as last time, we are done with this file
    if (! defined $logfile or $logfile eq $last_logfile) {
        ## Grab the next entry
        $fileinfo = $opt{file}[++$filenumber];
        ## No more file? We are done!
        last NEXTLOG if ! defined $fileinfo;
        # Otherwise, loop back with the new fileinfo
        redo NEXTLOG;
    }

    $last_logfile = $logfile;

    ## If this was a service, don't try to connect again if we already failed
    my $servicename;
    if ($logfile =~ /service=($servicenameregex)/i) {
        $servicename = $1;
        if (exists $badservice{$servicename}) {
            redo NEXTLOG;
        }
    }

    ## If 'yesfile' is set, skip anything that does not match at least one
    if (defined $arg{yesfile}->[0]) {
        if (! grep { $logfile =~ /$_/ } @{ $arg{yesfile} } ) {
            my $regex = join ', ' => sort @{ $arg{yesfile} };
            $arg{debug} > 1 and warn qq{Skipping file due to --yesfile arguments "$regex": $logfile\n};
            $arg{skippedfile}{$logfile} = 1;
            redo NEXTLOG;
        }
    }

    ## If 'nofile' is set, skip anything that matches any of them
    if (defined $arg{nofile}->[0]) {
        if ( grep { $logfile =~ /$_/ } @{ $arg{nofile} } ) {
            my $regex = join ', ' => sort @{ $arg{nofile} };
            $arg{debug} > 1 and warn qq{Skipping file due to --nofile arguments "$regex": $logfile\n};
            $arg{skippedfile}{$logfile} = 1;
            redo NEXTLOG;
        }
    }

    $arg{debug} and warn " Parsing file: $logfile\n";

    my $count = parse_file($logfile, $fileinfo);

    if ($count < 0) {
        if (-1 == $count) { ## skipfilebyregex was used
            $count = 0;
            ## Fallthrough
        }
        elsif (-2 == $count) { ## pg_stat_file has failed!
            ## Do not try this one again, even for different targets
            if (defined $servicename) {
                $badservice{$servicename} = 1;
            }
            push @problem_files => "!! Connection failure: $logfile";
            redo;
        }
        elsif (-3 == $count) { ## Reset argument was used
            $count = 0;
            ## Fallthrough
        }
        elsif (-4 == $count) { ## Size and mod_time have not changed
            $count = 0;
            ## Fallthrough
        }
        elsif (-5 == $count) { ## Given file does not exist
            push @problem_files => "!! File was not found: $logfile";
            ## Fallthrough
        }
        elsif (-6 == $count) { ## Given file is the wrong type!
            push @problem_files => "!! Not a regular file: $logfile";
            ## Fallthrough
        }
        elsif (-7 == $count) { ## Problem with pg_read_file
            push @problem_files => "!! Call to pg_read_file failed: $logfile";
            ## Fallthrough
        }
    }

    if ($count >= 0) {
        $found_errors += $count;
        push @files_parsed => [$logfile, $count];
        $fileorder{$logfile} = ++$filenum;
    }

    last if $arg{debug_partial_matches} and $count > $arg{debug_partial_matches};

    redo NEXTLOG;
}

my $important_problem_files = 0;
for my $problem (@problem_files) {
    next if $arg{skip_report_failing_host} and
        ($arg{skip_report_failing_host} eq 'all'
         or $problem =~ /\b$arg{skip_report_failing_host}\b/);
    $important_problem_files = 1;
}

## We're done parsing the message, send an email if needed
process_report() if $opt{grand_total} or $arg{mailzero} or $opt{mailzero} or $important_problem_files;
final_cleanup();

$arg{critical} and $found_errors >= $arg{critical} and exit 2;
$arg{warning}  and $found_errors >= $arg{warning}  and exit 1;
exit 0;


sub pick_log_file {

    ## Figure out which files we need to parse
    ## Sole argument is a hashref of file information:
    ##   name: logfile to open
    ##   original: original name
    ##   lastfile: we scanned last time this ran. May be an empty string or not exist

    my $info = shift;

    my $name = $info->{name} or die 'No name for the file found!';
    my $orig = $info->{original} or die 'No original file found!';
    my $lastfile = $info->{lastfile} || '';

    ## Basic flow:
    ## Start with "last" (and apply offset to it)
    ## Then walk forward until we hit the most recent one

    ## Handle the LATEST case right away
    if ($orig !~ /SERVICE=/ and $orig =~ s{([^/\\]*)LATEST([^/\\]*)$}{}o) {

        my ($prefix,$postfix) = ($1,$2);

        ## At this point, the lastfile has already been handled
        ## We need all files newer than that one, in order, until we run out

        ## If we don't have the list already, build it now
        if (! exists $opt{middle_filenames}) {

            my $dir = $orig;
            $dir =~ s{/\z}{};
            -d $dir or die qq{Cannot open $dir: not a directory!\n};
            opendir my $dh, $dir or die qq{Could not opendir "$dir": $!\n};

            ## We need the modification time of the lastfile
            my $lastfiletime = defined $lastfile ? -M $lastfile : 0;

            my %fileq;
            while (my $file = readdir($dh)) {
                my $fname = "$dir/$file";
                my $modtime = -M $fname;
                ## Skip if not readable
                next if ! -r _;
                if (length $prefix or length $postfix) {
                    next if $file !~ /\A\Q$prefix\E.*\Q$postfix\E\z/o;
                }
                ## Skip if it's older than the lastfile
                next if $lastfiletime and $modtime > $lastfiletime;
                $fileq{$modtime}{$fname} = 1;
            }
            closedir $dh or warn qq{Could not closedir "$dir": $!\n};

          TF: for my $time (sort { $a <=> $b } keys %fileq) {
                for my $file (sort keys %{$fileq{$time}}) {
                    push @{$opt{middle_filenames}} => $file;
                    ## If we don't have a lastfile, we simply use the most recent file
                    ## and throw away the rest
                    last TF if ! $lastfiletime;
                }
            }
        }

        ## Return the next file, or undef when we run out
        my $nextfile = pop @{ $opt{middle_filenames} };
        ## If we are done, remove this temp hash
        if (! defined $nextfile) {
            delete $opt{middle_filenames};
        }
        return $nextfile;

    } ## end of LATEST time travel

    ## No lastfile makes it easy
    return $name if ! $lastfile;

    ## If we haven't processed the lastfile, do that one first
    return $lastfile if ! exists $find{$lastfile};

    ## If the last is the same as the current, return the name
    return $name if $lastfile eq $name;

    ## We've processed the last file, are there any files in between the two?
    ## POSIX-based time travel
    if ($orig =~ /%/) {

        ## Build the list if we don't have it yet
        if (! exists $opt{middle_filenames}) {

            ## We're going to walk backwards, 30 minutes at a time, and gather up
            ## all files between "now" and the "last"
            my $timerewind = 60*30; ## 30 minutes
            my $maxloops = 24*2 * 7 * 60; ## max of 60 days
            my $bail = 0;
            my %seenfile;
            my $lastchecked = '';
          BACKINTIME: {

                my @ltime = localtime(time - $timerewind);
                my $newfile = strftime($orig, @ltime);
                if ($newfile ne $lastchecked) {
                    last if $newfile eq $lastfile;
                    $arg{debug} and warn "Checking for file $newfile (last was $lastfile)\n";
                    if (! exists $seenfile{$newfile}) {
                        $seenfile{$newfile} = 1;
                        push @{$opt{middle_filenames}} => $newfile;
                    }
                    $lastchecked = $newfile;
                }

                $timerewind += 60*30;
                ++$bail > $maxloops and die "Too many loops ($bail): bailing\n";
                redo;
            }

        }

        ## If the above loop found nothing, return the current name
        if (! exists $opt{middle_filenames}) {
            return $name;
        }

        ## Otherwise, pull it off the list until there is nothing left
        my $nextfile = pop @{ $opt{middle_filenames} };
        ## If we are done, remove this temp hash
        if (! defined $nextfile) {
            delete $opt{middle_filenames};
        }
        return $nextfile;
    }

    ## Just return the current file
    return $name;

} ## end of pick_log_file


sub parse_rc_files {

    ## Read in global settings from rc files

    my $file;
    if (! $arg{'no-tailnmailrc'}) {
        if ($arg{tailnmailrc}) {
            -e $arg{tailnmailrc} or die "Could not find the file $arg{tailnmailrc}\n";
            $file = $arg{tailnmailrc};
        }
        elsif (-e '.tailnmailrc') {
            $file = '.tailnmailrc';
        }
        elsif (exists $ENV{HOME} and -e "$ENV{HOME}/.tailnmailrc") {
            $file = "$ENV{HOME}/.tailnmailrc";
        }
        elsif (-e '/etc/tailnmailrc') {
            $file = '/etc/tailnmailrc';
        }
    }
    if (defined $file) {
        open my $rc, '<', $file or die qq{Could not open "$file": $!\n};
        while (<$rc>) {
            next if /^\s*#/;
            next if ! /^\s*([\w\_\-]+)\s*[=:]\s*(.+?)\s*$/o;
            my ($name,$value) = (lc $1,$2);
            ## Special case for leading and trailing whitespace
            $value =~ s/^"(.+)"$/$1/;
            $name =~ s/\-/_/g;
            $opt{$name} = $value;
            $arg{$name} = $value;
            ## If we are disabled, simply exit quietly
            if ($name eq 'disable' and $value) {
                exit 0;
            }
            if ($name eq 'maxsize') {
                $arg{maxsize} = $value;
            }
            if ($name eq 'duration_limit') {
                $arg{duration_limit} = $value;
            }
            if ($name eq 'tempfile_limit') {
                $arg{tempfile_limit} = $value;
            }
            if ($name =~ /^log_line_prefix([0-9]*)/) {
                my $suffix = $1 || 0;
                $opt{$name} = $arg{$name} = {} if ! ref $opt{$name};
                $opt{$name}{$suffix} = $value;
                $arg{$name}{$suffix} = $value;
            }
        }
        close $rc or die;
    }

    return;

} ## end of parse_rc_files


sub parse_config_file {

    ## Read in a configuration file and populate the global %opt

    ## Are we in the standard non-user comments at the top of the file?
    my $in_standard_comments = 1;

    ## Temporarily store user comments until we know where to put them
    my (@comment);

    ## Keep track of duplicate lines: ignore any but the first
    my %seenit;

    ## Store locally so we can easily populate %opt at the end
    my %localopt;

    open my $c, '<', $configfile or die qq{Could not open "$configfile": $!\n};
    $arg{debug} and warn qq{Opened config file "$configfile"\n};
    while (<$c>) {

        ## If we are at the top of the file, don't store standard comments
        if ($in_standard_comments) {
            next if /^## Config file for/;
            next if /^## This file is automatically updated/;
            next if /^## Last updated:/;
            next if /^\s*$/;
            ## Once we reach the first non-comment, non-whitespace line,
            ## treat it as a normal line
            $in_standard_comments = 0;
        }

        ## Found a user comment; store it away until we have context for it
        if (/^\s*#/) {
            push @comment => $_;
            next;
        }

        ## If the exact same line shows up more than once ignore it.
        ## Failing to do so will confuse the comment hash
        if (/^[\w]/ and $seenit{$_}++) {
            warn "Duplicate entry will be ignored at line $.: $_\n";
            next;
        }

        ## A non-comment after one or more comments allows us to map them to each other
        if (@comment and m{^([\w\_\-]+):}) {
            my $keyword = $1;
            my $line = $_;
            chomp $line;
            for my $c (@comment) {
                ## We store as both the keyword and the entire line
                push @{$itemcomment{$keyword}} => $c;
                push @{$itemcomment{$line}} => $c;
            }
            ## Empty out our user comment queue
            undef @comment;
        }

        ## What file(s) are we checking on?
        if (/^FILE([0-9]*):\s*(.+?)\s*$/) {

            my $suffix = $1 || 0;
            my $filename = $2;

            ## Basic sanity check
            if ($filename !~ /\w/) {
                die "No valid FILE found in the config file! (tried: $filename)\n";
            }

            ## If files were specified on the command line, use those instead
            if ($arg{file}[0]) {

                ## Have we been here already? Only need to override once
                if (! exists $localopt{filename}) {

                    for my $argfile (@{ $arg{file} }) {

                        ## If it contains a path, use it directly
                        if ($argfile =~ m{/}) {
                            $filename = $argfile;
                        }
                        ## Otherwise, replace the current file name but keep the directory
                        else {
                            my $dir = dirname($filename);
                            $filename = "$dir/$argfile";
                        }

                        ## Add it to our local list both as final and original name
                        push @{ $localopt{file} } => {
                            name => $filename,
                            original => $filename,
                            commandline => 1,
                            lastfile => '',
                            offset => 0,
                        };
                    }

                    next;
                }
            }

            ## If the file contains % escapes, replace with the actual time
            my $newfilename = transform_filename($filename);

            ## Save to the local list, storing the original filename for config rewriting
            push @{ $localopt{file} } =>
                {
                 name => $newfilename,
                 original => $filename,
                 suffix => $suffix,
                 };
        } ## end of FILE:

        ## The last filename we used
        elsif (/^LASTFILE([0-9]*):\s*(.+?)\s*$/) {
            my $suffix = $1 || 1;
            $localopt{lastfile}{$suffix} = $2;
        }
        ## The rotation file modification time
        elsif (/^ROTATEFILEMODTIME([0-9]*):\s*(.+?)\s*$/) {
            my $suffix = $1 || 1;
            $localopt{rotatefilemodtime}{$suffix} = $2;
        }
        ## Who to send emails to for this file
        elsif (/^EMAIL:\s*(.+?)\s*$/) {
            push @{$localopt{email}}, $1;
        }
        ## Who to send emails from
        elsif (/^FROM:\s*(.+?)\s*$/) {
            $localopt{from} = $1;
        }
        ## What type of report this is
        elsif (/^TYPE:\s*(.+?)\s*$/) {
            $arg{type} = $1;
        }
        ## Exclude durations below this number
        elsif (/^DURATION:\s*([0-9]+)/) {
            ## Command line still wins (default is negative number)
            if ($arg{duration} < 0) {
                $arg{duration} = $localopt{duration} = $1;
            }
        }
        ## Limit how many duration matches we show
        elsif (/^DURATION_LIMIT:\s*([0-9]+)/) {
            ## Command line still wins
            if ($arg{duration_limit} < 0) {
                $arg{duration_limit} = $localopt{duration_limit} = $1;
            }
        }
        ## Exclude tempfiles below this number
        elsif (/^TEMPFILE:\s*([0-9]+)/) {
            ## Command line still wins
            if ($arg{tempfile} < 0) {
                $arg{tempfile} = $localopt{tempfile} = $1;
            }
        }
        ## Limit how many tempfile matches we show
        elsif (/^TEMPFILE_LIMIT:\s*([0-9]+)/) {
            ## Command line still wins
            if (!$arg{tempfile_limit}) {
                $arg{tempfile_limit} = $localopt{tempfile_limit} = $1;
            }
        }
        elsif (/^LOG.LINE.PREFIX([0-9]*):\s*(.+)/) {
          my $suffix = $1 || 0;
          ## Force arg to new style if needed
          $arg{log_line_prefix} = {} if ! ref $arg{log_line_prefix};
          $arg{log_line_prefix}{$suffix} = $localopt{log_line_prefix}{$suffix} = $2;
        }
        elsif (/^LOG.LINE.PREFIX.REGEX:\s*(.+)/) {
          my $regex = $1;
          if ($regex !~ /([a-zA-z].*) = (.+)/) {
              die "Invalid regex given for $_\n";
          }
          $arg{log_line_prefix} = {} if ! ref $arg{log_line_prefix};
          $arg{log_line_prefix}{$1} = $localopt{log_line_prefix}{$1} = $2;
        }
        ## How to sort the output
        elsif (/^SORTBY:\s*(\w+)/) {
            $localopt{sortby} = $1;
        }
        ## Force line number lookup on or off
        elsif (/^FIND_LINE_NUMBER:\s*([0-9]+)/) {
            $arg{find_line_number} = $localopt{find_line_number} = $1;
        }
        ## What type of Postgres log file
        elsif (/^PGLOG:\s*(\w+)/) {
            $localopt{pglog} = $1;
        }
        ## Any inheritance files to look at
        elsif (/^INHERIT:\s*(.+)/) {
            push @{$localopt{inherit}}, $1;
        }
        ## Which lines to exclude from the report
        elsif (/^EXCLUDE:\s*(.+?)\s*$/) {
            push @{$localopt{exclude}}, $1;
        }
        ## Which prefix lines to exclude from the report
        elsif (/^EXCLUDE_PREFIX:\s*(.+?)\s*$/) {
            push @{$localopt{exclude_prefix}}, $1;
        }
        ## Which lines to exclude from the report
        elsif (/^EXCLUDE_NON_PARSED:\s*(.+?)\s*$/) {
            push @{$localopt{exclude_non_parsed}}, $1;
        }
        ## Which prefix lines to exclude from the report
        elsif (/^EXCLUDE_VIA_FILE:\s*(.+?)\s*$/) {
            push @{$localopt{exclude_via_file}}, $1;
        }
        ## Which lines to include in the report
        elsif (/^INCLUDE:\s*(.+)/) {
            push @{$localopt{include}}, $1;
        }
        ## Which lines to include in the report
        elsif (/^INCLUDE_VIA_FILE:\s*(.+)/) {
            push @{$localopt{include_via_file}}, $1;
        }
        ## Custom flattening rules: format is FOO/BAR which maps to s/FOO/BAR/g;
        elsif (/^FLATTEN:\s*(.+)/) {
            push @{$localopt{flatten}}, $1;
        }
        ## The current offset into a file
        elsif (/^OFFSET([0-9]*):\s*([0-9]+)/) {
            my $suffix = $1 || 1;
            $localopt{offset}{$suffix} = $2;
        }
        ## The last modification time for this file
        elsif (/^MODTIME([0-9]*):\s*(.+)/) {
            my $suffix = $1 || 1;
            $localopt{modtime}{$suffix} = $2;
        }
        ## The custom maxsize for all files
        elsif (/^MAXSIZE:\s*([0-9]+)/) {
            $localopt{maxsize} = $1;
        }
        ## The subject line
        elsif (/^MAILSUBJECT:\s*(.+)/) { ## Trailing whitespace is significant here
            $localopt{mailsubject} = $1;
            $localopt{customsubject} = 1;
        }
        ## Force mail to be sent - overrides any other setting
        elsif (/^MAILZERO:\s*(.+)/) {
            $localopt{mailzero} = $1;
        }
        ## Allow (possibly multiple) mail signatures
        elsif (/^MAILSIG:\s*(.+)/) {
            push @{$localopt{mailsig}}, $1;
            push @{$arg{mailsig}}, $1;
        }
        ## Size at which we cutoff long statements
        elsif (/^STATEMENT_SIZE:\s*(.+)/) {
            $localopt{statement_size} = $1;
        }
        ## Prevent the inherited file from setting certain things
        elsif (/NO.INHERIT:\s*(.+)/) {
            $opt{no_inherit} = $1;
        }
        ## Size at which we cutoff nonparsed long statements
        elsif (/^NONPARSED_STATEMENT_SIZE:\s*(.+)/) {
            $localopt{nonparsed_statement_size} = $1;
        }
        ## File mode
        elsif (/^PGMODE:\s*(.+)/) {
            $localopt{pgmode} = $1;
        }
    }
    close $c or die qq{Could not close "$configfile": $!\n};

    ## Adjust the file suffixes as needed
    ## This allows us to simply add multiple bare 'FILE:' entries before the first rewrite
    ## We also plug in the LASTFILE, OFFSET, MODTIME, and ROTATEFILEMODTIME values now
    my %numused;
    for my $file (@{ $localopt{file} }) {
        $file->{suffix} ||= 0;
        next if ! $file->{suffix};
        if ($numused{$file->{suffix}}++) {
            die "The same FILE suffix ($file->{suffix}) was used more than once!\n";
        }
    }
    for my $file (@{ $localopt{file} }) {

        ## No need to change anything if we forced via the command line
        next if $file->{commandline};

        ## Only need to adjust 0s
        if (! $file->{suffix}) {

            ## Replace with the first free number
            my $x = 1;
            {
                if (! $numused{$x}++) {
                    $file->{suffix} = $x;
                    last;
                }
                if ($x++ > 999) {
                    die "Something went wrong: 999 iterations to find a FILE suffix!\n";
                }
                redo;
            }
        }

        ## Put the lastfile into place if it exists
        $file->{lastfile} = $localopt{lastfile}{$file->{suffix}} || '';

        ## Put the offset into place if it exists
        $file->{offset} = $localopt{offset}{$file->{suffix}} || 0;

        ## Put the modtime into place if it exists
        $file->{modtime} = $localopt{modtime}{$file->{suffix}} || '';

        ## Put the rotation file modtime into place if it exists
        $file->{rotatefilemodtime} = $localopt{rotatefilemodtime}{$file->{suffix}} || '';

    }

    ## Move the local vars into place, also record that we found them here
    for my $k (keys %localopt) {
        ## Note it came from the config file so we rewrite it there
        $opt{configfile}{$k} = 1;
        ## If an array, we also want to mark individual items
        if (ref $localopt{$k} eq 'ARRAY') {
            for my $ik (@{$localopt{$k}}) {
                $opt{configfile}{"$k.$ik"} = 1;
            }
        }
        $opt{$k} = $localopt{$k};
    }

    if ($arg{debug}) {
        local $Data::Dumper::Varname = 'opt';
        warn Dumper \%opt;
        local $Data::Dumper::Varname = 'arg';
        warn Dumper \%arg;
    }

    return;

} ## end of parse_config_file


sub parse_inherited_files {

    ## Call parse_inherit_file on each item in $opt{inherit}

    for my $file (@{$opt{inherit}}) {
        parse_inherit_file($file);
    }

    return;

} ## end of parse_inherited_files


sub parse_inherit_file {

    ## Similar to parse_config_file, but much simpler
    ## Because we only allow a few items
    ## This is most useful for sharing INCLUDE and EXCLUDE across many config files

    my $file = shift;

    ## Only allow certain characters.
    if ($file !~ s{^\s*([a-zA-Z0-9_\.\/\-\=]+)\s*$}{$1}) {
        die "Invalid inherit file ($file)\n";
    }

    ## If not an absolute path, we'll check current directory and "tnm/"
    my $filename = $file;
    my $filefound = 0;
    if (-e $file) {
        $filefound = 1;
    }
    elsif ($file =~ /^\w/) {
        $filename = "tnm/$file";
        if (-e $filename) {
            $filefound = 1;
        }
        else {
            my $basedir = dirname($0);
            $filename = "$basedir/$file";
            if (-e $filename) {
                $filefound = 1;
            }
            else {
                $filename = "$basedir/tnm/$file";
                if (-e $filename) {
                    $filefound = 1;
                }
                else {
                    ## Use the config file's directory
                    my $basedir2 = dirname($configfile);
                    $filename = "$basedir2/$file";
                    if (-e $filename) {
                        $filefound = 1;
                    }
                    else {
                        $filename = "basedir2/tnm/$file";
                        if (-e $filename) {
                            $filefound = 1;
                        }
                        else {
                            ## Try the home directory/tnm
                            $filename = "$ENV{HOME}/tnm/$file";
                            -e $filename and $filefound = 1;
                        }
                    }
                }
            }
        }
    }
    if (!$filefound) {
        die "Unable to open inherit file ($file)\n";
    }

    open my $fh, '<', $filename or die qq{Could not open file "$file": $!\n};
    while (<$fh>) {
        chomp;
        next if /^#/ or ! /\w/;
        ## Only a few things are allowed in here
        if (/^FIND_LINE_NUMBER:\s*([0-9]+)/) {
            ## We adjust the global here and now
            $arg{find_line_number} = $1;
        }
        ## Custom flattening rules
        elsif ( /^FLATTEN:\s*(.+)/ ) {
            push @{$opt{flatten}}, $1;
        }
        ## What type of Postgres log we are parsing
        elsif (/^PGLOG:\s*(\w+)/) {
            $opt{pglog} = $1;
        }
        ## How to sort the output
        elsif (/^SORTBY:\s*(\w+)/) {
            $opt{sortby} = $1;
        }
        ## Which lines to exclude from the report
        elsif (/^EXCLUDE:\s*(.+?)\s*$/) {
            push @{$opt{inherited_exclude}}, $1;
        }
        ## Which prefix lines to exclude from the report
        elsif (/^EXCLUDE_PREFIX:\s*(.+?)\s*$/) {
            push @{$opt{inherited_exclude_prefix}}, $1;
        }
        ## Which lines to exclude from the report
        elsif (/^EXCLUDE_NON_PARSED:\s*(.+?)\s*$/) {
            push @{$opt{inherited_exclude_non_parsed}}, $1;
        }
        ## Which lines to exclude - but only for some files
        elsif (/^EXCLUDE_VIA_FILE:\s*(.+?)\s*$/) {
            push @{$opt{exclude_via_file}}, $1;
        }
        ## Which lines to include in the report
        elsif (/^INCLUDE:\s*(.+)/) {
            push @{$opt{inherited_include}}, $1;
        }
        ## Which lines to include in the report - limited to file
        elsif (/^INCLUDE_VIA_FILE:\s*(.+)/) {
            push @{$opt{inherited_via_file}}, $1;
        }
        ## Maximum file size
        elsif (/^MAXSIZE:\s*([0-9]+)/) {
            $opt{maxsize} = $1;
        }
        ## Exclude durations below this number
        elsif (/^DURATION:\s*([0-9]+)/) {
            ## Command line still wins
            if ($arg{duration} < 0) {
                $arg{duration} = $1;
            }
        }
        ## Duration limit
        elsif (/^DURATION_LIMIT:\s*([0-9]+)/) {
            ## Command line still wins
            if ($arg{duration_limit} < 0) {
                $arg{duration_limit} = $1;
            }
        }
        ## Exclude tempfiles below this number
        elsif (/^TEMPFILE:\s*([0-9]+)/) {
            ## Command line still wins
            if ($arg{tempfile} < 0) {
                $arg{tempfile} = $1;
            }
        }
        ## Tempfile limit
        elsif (/^TEMPFILE_LIMIT:\s*([0-9]+)/) {
            ## Command line still wins
            $arg{tempfile_limit} ||= $1;
        }
        ## Who to send emails from
        elsif (/^FROM:\s*(.+?)\s*$/) {
            $opt{from} = $1;
        }
        ## Who to send emails to for this file
        elsif (/^EMAIL:\s*(.+?)\s*$/) {
            push @{$opt{email}}, $1;
        }
        ## Force mail to be sent - overrides any other setting
        elsif (/^MAILZERO:\s*(.+)/) {
            $opt{mailzero} = $1;
        }
        ## The file to use
        elsif (/^FILE([0-9]*):\s*(.+)/) {

            my $suffix = $1 || 0;
            my $lfilename = $2;

            ## Skip entirely if we have a command-line file request
            ## This is handled in the main config parsing
            next if $arg{file}[0];

            ## As with the normal config file, store a temp version
            ## Save to the local list, storing the original filename for config rewriting
            push @{ $opt{tempifile} } =>
                {
                 original => $lfilename,
                 suffix => $suffix,
                 };
        }
        ## The mail subject
        elsif (/^MAILSUBJECT:\s*(.+)/) {
            $opt{mailsubject} = $1;
            $opt{customsubject} = 1;
        }
        ## The mail signature
        elsif (/^MAILSIG:\s*(.+)/) {
            push @{$opt{mailsig}}, $1;
        }
        ## The log line prefix
        elsif (/^LOG.LINE.PREFIX([0-9]*):\s*(.+)/o) {
          my $suffix = $1 || 0;
          $opt{log_line_prefix} = {} if ! ref $opt{log_line_prefix};
          $opt{log_line_prefix}{$suffix} = $2;
        }
        elsif (/^LOG.LINE.PREFIX.REGEX:\s*(.+)/) {
          my $regex = $1;
          if ($regex !~ /([a-zA-z].*) = (.+)/) {
              die "Invalid regex given for $_\n";
          }
          $opt{log_line_prefix} = {} if ! ref $opt{log_line_prefix};
          $opt{log_line_prefix}{$1} = $2;
        }
        ## Size at which we cutoff long statements
        elsif (/^STATEMENT_SIZE:\s*(.+)/) {
            $opt{statement_size} = $1;
        }
        ## Size at which we cutoff nonparsed long statements
        elsif (/^NONPARSED_STATEMENT_SIZE:\s*(.+)/) {
            $opt{nonparsed_statement_size} = $1;
        }
        ## File mode
        elsif (/^PGMODE:\s*(.+)/) {
            $opt{pgmode} = $1;
        }
        else {
            warn qq{Unknown item in include file "$file": $_\n};
        }

    }
    close $fh or warn qq{Could not close file "$file": $!\n};

    ## Merge all the "FILE" entries and adjust suffixes
    ## We allow overlap between the normal and inherited lists

    if (exists $opt{tempifile}) {

        my %numused;
        for my $file (@{ $opt{tempifile} }) {
            $file->{suffix} ||= 0;
            next if ! $file->{suffix};
            if ($numused{$file->{suffix}}++) {
                die "The same FILE suffix ($file->{suffix}) was used more than once inside $filename!\n";
            }
        }

        for my $file (@{ $opt{tempifile} }) {

            ## Change zero to the first free number
            if (! $file->{suffix}) {
                my $x = 1;
                {
                    if (! $numused{$x}++) {
                        $file->{suffix} = $x;
                        last;
                    }
                    if ($x++ > 999) {
                        die "Something went wrong: 999 iterations to find a FILE suffix inside $filename!\n";
                    }
                    redo;
                }
            }

            ## Create our real entry
            push @{ $opt{file} } =>
                {
                 name      => transform_filename($file->{original}),
                 original  => $file->{original},
                 suffix    => $file->{suffix},
                 inherited => 1,
                 lastfile  => '',
                 offset    => 0,
                 };
        }

        ## Remove our temporary list
        delete $opt{tempifile};
    }

    return;

} ## end of parse_inherited_file


sub parse_state_file {

    ## Gather ephemeral information for this run

    ## This file is just the config file with an addition of ".state"
    if (! -e $statefile) {
        $arg{verbose} and print "No state file found: $statefile\n";
        return;
    }

    open my $sfh, '<', $statefile or die qq{Could not open "$statefile": $!\n};
    $arg{debug} and warn qq{Opened state file "$statefile"\n};
    my %stateinfo;
    while (<$sfh>) {

        ## Skip comments
        next if /^#/;

        ## We do not technically need this, but it is included anyway to make
        ## the state file more readable
        if (/^FILE([0-9]*):\s*(.+?)\s*$/) {
            my $suffix = $1 || 1;
            $stateinfo{$suffix}{file} = $2;
        }
        ## The last version of this file we used
        elsif (/^LASTFILE([0-9]*):\s*(.+?)\s*$/) {
            my $suffix = $1 || 1;
            $stateinfo{$suffix}{lastfile} = $2;
        }
        ## The last modification time for this file
        elsif (/^MODTIME([0-9]*):\s*(.+)/) {
            my $suffix = $1 || 1;
            $stateinfo{$suffix}{modtime} = $2;
        }
        ## The current offset into a file
        elsif (/^OFFSET([0-9]*):\s*([0-9]+)/) {
            my $suffix = $1 || 1;
            $stateinfo{$suffix}{offset} = $2;
        }
        ## The rotation file modification time
        elsif (/^ROTATEFILEMODTIME([0-9]*):\s*(.+?)\s*$/) {
            my $suffix = $1 || 1;
            $stateinfo{$suffix}{rotatefilemodtime} = $2;
        }
        elsif (/[a-zA-Z]/) {
            warn qq{Unknown item in state file "$statefile": $_\n};
        }

    }
    close $sfh or warn qq{Could not close file "$statefile": $!\n};

    ## Walk through our existing ones and see if we can find state information for it
    for my $file (@{ $opt{file} }) {
        my $suffix = $file->{'suffix'} or next;
        next if ! exists $stateinfo{$suffix};
        for my $info (qw/ lastfile modtime offset rotatefilemodtime /) {
            if (exists $stateinfo{$suffix}{$info}) {
                $file->{$info} = delete $stateinfo{$suffix}{$info};
            }
        }
    }

    return;

} ## end of parse_state_file


sub parse_filehandle {

    ## Scan a filehandle for lines of interest
    ## Arguments: five
    ## 1. File handle
    ## 2. File name
    ## 3. PID line mapping (hashref)
    ## 4. Local file info (hashref)
    ## 5. Map of pids to numbers (hashref)
    ## Returns: number of matches, number of lines processed, any extra string

    my $fh = shift or die;
    my $filename = shift;
    my $pids = shift;
    my $lfileinfo = shift;
    my $current_pid_num = shift;
    my $linenumber = shift || 0;
    my $start_time = $arg{start_time} || 0;

    my $newlines = 0;
    my $count = 0;

    ## Switch to CSV mode if required or if the file ends in '.csv'
    if (lc $pglog eq 'csv' or $filename =~ /\.csv$/) {
        if (! defined $csv) {
            eval {
                require Text::CSV;
            };
            if (!$@) {
                $csv = Text::CSV->new({ binary => 1, eol => $/ });
            }
            else {
                ## Assume it failed because it doesn't exist, so try another version
                eval {
                    require Text::CSV_XS;
                };
                if ($@) {
                    die qq{Cannot parse CSV logs unless Text::CSV or Text::CSV_XS is available\n};
                }
                $csv = Text::CSV_XS->new({ binary => 1, eol => $/ });
            }
        }
        $linenumber = 1;
        while (my $line = $csv->getline($fh)) {
            my $prefix = "$line->[0] [$line->[3]]";
            my $context = length $line->[18] ? "CONTEXT: $line->[18] " : '';
            my $raw = "$line->[11]:  $line->[13] ${context}STATEMENT:  $line->[19]";
            $count += process_line({pgprefix => $prefix, rawstring => $raw, line => $linenumber}, $., $filename);
            my $innerlines = $raw =~ y/\n/\n/;
            $linenumber += 1 + $innerlines;
        }

    } ## end of PG CSV mode
    else {
        ## Postgres-specific multi-line grabbing stuff:
        my ($pgts, $pgpid, $lastpid, $pgprefix);
        my $pgnum = 1;
        my $lastline = '';
        my $syslognum = 0; ## used by syslog only
        my $bailout = 0; ## emergency bail out in case we end up sleep seeking
        my $ignore_indent = 0; ## ignore indented lines, e.g. continuations after a skipped LOG

        my $prefixnumber = $lfileinfo->{suffix};

        ## A regex llp will override any number-based matching
        for my $regexname (grep { /^[a-zA-Z]/ } sort keys %pgpidre) {
            if ($filename =~ /$regexname/) {
                $prefixnumber = $regexname;
                last;
            }
        }

        $prefixnumber = 0 if ! exists $pgpidre{$prefixnumber};

        my $pgpidre1 = $pgpidre{$prefixnumber}{1};
        my $pgpidre2 = $pgpidre{$prefixnumber}{2};
        my $pgpidre3 = $pgpidre{$prefixnumber}{3};

      LOGLINE: while (<$fh>) {

            if ($arg{debug_partial_matches} and $count > $arg{debug_partial_matches}) {
                return $count, $linenumber, undef;
            }

            ## For service files, we need to return this string if we are at the end of the file
            if (eof $fh and $filename =~ /SERVICE=/) {
                return $count, $linenumber, $_;
            }

            $linenumber++;

            ## We ran into a truncated line last time, so we are most likely done
            last if $bailout;

            ## If we are ignoring indented lines, keep slurping until there are no more
            if ($ignore_indent and /^\t/) {
              next LOGLINE;
            }
            $ignore_indent = 0;

            ## Easiest to just remove the newline here and now
            if (! chomp) {
                ## There was no newline, so it's possible some other process is in
                ## the middle of writing this line. Just in case this is so, sleep and
                ## let it finish, then try again. Because we don't want to turn this
                ## into a tail -f situation, bail out of the loop once done
                sleep 1;
                ## Rewind just far enough to try this line again
                seek $fh, - (length $_), 1;
                $_ = <$fh>;
                if (! chomp) {
                    ## Still no go! Let's just leave and abandon this line
                    last LOGLINE;
                }
                ## Success! Finish up this line, but then abandon any further slurping
                $bailout = 1;
            }

            if ($start_time and /^([0-9: -]+)/) {
                my $current = $1 =~ s/\D//gr;
                next LOGLINE if $current < $start_time;
                $start_time = 0;
            }

            if ($pgmode) {

                ## 1=prefix 2=timestamp 3=PID 4=rest
                if ($_ =~ s/$pgpidre1/$4/) {
                    ## We want the timestamp and the pid, even if we have to fake it
                    ($pgprefix,$pgts,$pgpid,$pgnum) = ($1, $2||'', $3||1, 1);
                    $pgprefix =~ s/\s+$//o;
                    if ($pglog =~ /syslog/) {
                        if ($pgprefix =~ /: \[([0-9]+)/) {
                            $pgnum = $1;
                        }
                    }
                    $lastpid = $pgpid;

                    ## Have we seen this PID before?
                    if (exists $pids->{$pgpid}) {
                        if ($pglog =~ /syslog/) {
                            if ($pglog eq 'syslog' and $syslognum and $syslognum != $pgnum) {
                                ## Got a new statement, so process the old
                                $count += process_line(delete $pids->{$pgpid}, 0, $filename);
                            }
                        }
                        else {
                            ## Append to the string for this PID
                            if (/\b(?:STATEMENT|DETAIL|HINT|CONTEXT|QUERY):  /o) {
                                ## Increment the pgnum by one
                                $pgnum = $current_pid_num->{$pgpid} + 1;
                            }
                            else {
                                ## Process the old one
                                ## Delete it so it gets recreated afresh below
                                $count += process_line(delete $pids->{$pgpid}, 0, $filename);
                            }
                        }
                    }

                    if ($pglog =~ /syslog/) {
                        $syslognum = $pgnum;
                        ## Increment our arbitrary internal number
                        $current_pid_num->{$pgpid} ||= 0;
                        $pgnum = $current_pid_num->{$pgpid} + 1;
                    }

                    ## Optionally strip out SQLSTATE codes
                    if ($arg{sqlstate}) {
                        $_ =~ s/^(?:FATAL|ERROR):  ([0-9A-Z]{5}): /ERROR:  /o;
                    }

                    ## Assign this string to the current pgnum slot
                    $pids->{$pgpid}{string}{$pgnum} = $_;
                    $current_pid_num->{$pgpid} = $pgnum;

                    ## If we don't yet have a line, store it, plus the prefix and timestamp
                    if (! $pids->{$pgpid}{line}) {
                        $pids->{$pgpid}{line} = ($linenumber + $newlines);
                        $pids->{$pgpid}{pgprefix} = $pgprefix;
                        $pids->{$pgpid}{pgtime} = $pgts;
                    }

                    ## Remember this line
                    $lastline = $_;

                    ## We are done: go the next line
                    next LOGLINE;
                }

                ## We did not match the log_line_prefix

                ## May be a continuation or a special LOG line (e.g. autovacuum)
                ## If it is, we'll simply ignore it
                if ($_ =~ m{$pgpidre2}) {
                    if ($arg{debug} > 2) {
                        warn "Skipping line $_\n";
                    }
                    $ignore_indent = 1;
                    next LOGLINE;
                }

                ## If we do not have a PID yet, skip this line
                next LOGLINE if ! $lastpid;

                ## If there is a leading tab, remove it and treat as a continuation
                if (s{^\t}{ }) {
                    ## Increment the pgnum
                    $pgnum = $current_pid_num->{$lastpid} + 1;
                    $current_pid_num->{$lastpid} = $pgnum;

                    ## Store this string
                    $pids->{$lastpid}{string}{$pgnum} = $_;
                }
                ## May be a special LOG entry with no %u@%d etc.
                ## We simply skip these ones if they go into a LOG:
                elsif ($_ =~ m{$pgpidre3\s+LOG:}) {
                    ## Skip any subsequent indented lines
                    $ignore_indent = 1;
                    next LOGLINE;
                }
                else {
                    ## Not a continuation, so probably an error from the OS
                    ## Simply parse it right away, force it to match
                    if (! $arg{skip_non_parsed}) {
                        $count += process_line($_, $linenumber + $newlines, $filename, 1, 'nonparsed');
                    }
                }

                ## No need to do anything more right now if in pgmode
                next LOGLINE;

            } ## end of normal pgmode

            ## Just a bare entry, so process it right away
            $count += process_line($_, $linenumber + $newlines, $filename);

        } ## end of each line in the file

    } ## end of non-CSV mode

    return $count, $linenumber, undef;

} ## end of parse_filehandle


sub parse_file {

    ## Parse a file - this is the workhorse
    ## Arguments: two
    ## 1. Exact filename we are parsing
    ## 2. Hashref of file information:
    ##   name: logfile to open
    ##   original: original name
    ##   lastfile: we scanned last time this ran. May be an empty string or not exist
    ##   offset: where in the file we stopped at last time
    ## Returns the number of matches, or
    ## -1: File skipped due to "skipfilebyregex" option
    ## -2: Call to pg_stat_file failed: usually means server is unreachable
    ## -3: Reset argument was used, so we did not really parse
    ## -4: Skipped as mod time and size have not changed
    ## -5: The file does not exist
    ## -6: The file exists, but is not a regular file
    ## -7: There was an unknown problem with pg_read_file

    my $filename = shift;
    my $lfileinfo = shift;

    ## The file we scanned last time we ran
    my $lastfile = $lfileinfo->{lastfile} || '';

    ## Set this as the latest (but not the lastfile)
    $lfileinfo->{latest} = $filename;

    ## Touch the hash so we know we've been here
    $find{$filename} = {};

    ## We may want to skip this file entirely
    if (exists $arg{skipfilebyregex}) {
        for my $regex (@{ $arg{skipfilebyregex} }) {
            if ($filename =~ /$regex/) {
                $arg{verbose} and warn "Skipping file $filename as it matched regex $regex\n";
                return -1;
            }
        }
    }

    ## Keep track of matches for this file
    my $count = 0;

    ## Needed to track postgres PIDs
    my %pidline;

    my $fh;

    my $newoffset = 0;
    my $newmodtime = '';

    ## Allow for alternate service and password files
    my $basedir = dirname($0);

    my $tnm_service_file = 'tnm.service.conf';
    ## Command line always wins
    if ($arg{service_conf_file}) {
        -e $arg{service_conf_file} or die "Specified service.conf file does not exist: $arg{service_conf_file}\n";
        $ENV{PGSERVICEFILE} = $arg{service_conf_file};
    }
    elsif (-e "$basedir/$tnm_service_file") {
      $ENV{PGSERVICEFILE} = "$basedir/$tnm_service_file";
    }

    my $tnm_pgpass_file = 'tnm.pgpass';
    if ($arg{pgpass_file}) {
        -e $arg{pgpass_file} or die "Specified pgpass file does not exist: $arg{pgpass_file}\n";
        $ENV{PGPASSFILE} = $arg{pgpass_file};
    }
    elsif (-e "$basedir/$tnm_pgpass_file") {
      $ENV{PGPASSFILE} = "$basedir/$tnm_pgpass_file";
    }

    ## Handle alternate ways of reading in a file
    ## At the end of the day, we want to provide a pseudo-filehandle
    if ($filename =~ /SERVICE=/) {

        (my $newfilename = $filename) =~ s/SERVICE=($servicenameregex)\s*//;
        my $service = $1;

        if ($newfilename =~ m{(.+)/LATEST}) { ## Note: a directory is mandatory
            my $logdir = $1;
            ## Quick sanity check: may exclude some real-world cases (raise a github issue!)
            $logdir =~ m{^[A-Za-z0-9/_]+$} or die "Invalid directory: >>$logdir<<\n";
            $newfilename = get_latest_service_file($service, $logdir);
            $arg{debug} and warn "Setting newfilename to $newfilename via get_latest_service_file\n";
        }

        ## Get information about this file
        my $remotefileinfo = call_pg_stat_file($service, $newfilename);
        return -2 if ! ref $remotefileinfo;

        $newmodtime = $remotefileinfo->{modification} || '';

        ## If the reset argument was used, don't parse the file, just set the new offset and modtime
        if ($arg{reset}) {
            $arg{verbose} and warn "  Resetting offset to 0\n";
            if ($lfileinfo->{offset} != 0) {
                $opt{newoffset}{$filename} = 0;
                $opt{newmodtime}{$filename} = $newmodtime;
            }
            return -3;
        }

        ## Make a decision soon about whether to bother trying to read the .1 file
        my $read_rotated = 1; ## 1=might need  0=do not need  2=need

        ## Offset of the file we are going to read
        my $offset = $lfileinfo->{offset} || 0;

        my $filesize = $remotefileinfo->{size};

        ## If we passed in a file manually, always read the entire thing (and do not worry about file rotation)
        if ($arg{file}[0]) {
            $offset = 0;
            $read_rotated = 0;
        }

        ## If the offset was set from the command line, use that
        if ($arg{offset} != -1) {
            $read_rotated = 0;
            if ($arg{offset} >= 0) {
                $offset = $arg{offset};
                if ($offset > $filesize) {
                    $arg{verbose} and warn "  Changing offset ($offset) to file size $filesize\n";
                    $offset = $filesize;
                }
            }
            else { ## Negative offset means we walk back from end of the file
                $offset = $filesize + $arg{offset};
                ## But obviously don't walk too far back!
                if ($offset < 0) {
                    $arg{verbose} and warn "  Changing offset to 0 (file size was only $filesize)\n";
                    $offset = 0;
                }
            }
        }
        ## If start_time was set, read in both files from the start
        elsif ($arg{start_time}) {
            $read_rotated = 3;
            $offset = 0;
            delete $lfileinfo->{rotatefilemodtime};
        }
        ## If the file is smaller than our offset, assume it's a new file
        elsif ($filesize < $offset) {
            $arg{debug} and print "File has shrunk, so resetting offset to 0\n";
            $read_rotated = 2;
        }
        ## If the mod time and size are the same, do nothing at all
        elsif ($lfileinfo->{modtime} and $lfileinfo->{modtime} eq $newmodtime and $offset == $filesize
               and !$arg{debug_force_rotated}) {
            $arg{verbose} and print "Skipping file '$filename', as modtime is still $newmodtime and size is still $filesize\n";
            return -4;
        }

        if ($arg{debug_force_rotated}) {
            $read_rotated = 2;
            $offset = 0;
            $lfileinfo->{rotatefilemodtime} = 'forced';
        }

        ## Get exclusion and inclusion regexes for this file
        ($exclude,$include,$exclude_prefix,$exclude_non_parsed,$bonus_include) = generate_regexes($filename);

        ## We may want to check for a .1 file and get information about it
        ## Just in case, skip if we already end in ".1"!
        my $remoterotatedfileinfo = {};
        if ($read_rotated and $filename !~ /\.1/) {

            my $rotated_newfilename = "$newfilename.1";

            ## Use pg_ls_dir to verify the file is there before we try pg_stat_file
            my $remote_file_list = call_pg_ls_dir( $service, dirname($newfilename) );
            return -2 if ! ref $remote_file_list;

            if (! exists $remote_file_list->{$rotated_newfilename}) {
                $opt{newrotatefilemodtime}{$filename} = 'rotated_file_gone';
            }
            else {

                $remoterotatedfileinfo = call_pg_stat_file($service, "$rotated_newfilename");

                ## This should be caught by the pg_ls_dir above, but nonetheless:
                if ($remoterotatedfileinfo =~ /could not stat file/) {
                    $opt{newrotatefilemodtime}{$filename} = 'rotated_file_gone';
                }
                elsif (! ref $remoterotatedfileinfo) {
                    return -2;
                }
            }

            if (($opt{newrotatefilemodtime}{$filename} // '') eq 'rotated_file_gone') {
                ## The rotated file begin gone usually indicates a rebuilt server
                ## In this case, it makes sense to set the offset to 0 for the main file
                $arg{verbose} and warn "File $rotated_newfilename not found, so setting offset to 0\n";
                $offset = 0 if $lfileinfo->{rotatefilemodtime};
            }
        }

        ## If there is an offset, it will be from the older file: which is probably the rotated file
        ## Thus, we keep any offset, and read the entire "current" file
        if (
            ref $remoterotatedfileinfo ## if it gave an error above, this will be a scalar
            and exists $remoterotatedfileinfo->{modification} and length $remoterotatedfileinfo->{modification} ## rotated file exists AND
            and (!exists $lfileinfo->{rotatefilemodtime} ## we either have never seen this file ...
                 or ($remoterotatedfileinfo->{modification} ne $lfileinfo->{rotatefilemodtime})) ## OR it has changed since the last time we have
           ) {
            my $rotatesize = $arg{debug_partial_read} || $remoterotatedfileinfo->{size};
            my $retval = slurp_via_pg_read_file($service, "$newfilename.1", $offset, $rotatesize, \%pidline, $filename, $lfileinfo);
            return -7 if $retval < 0;
            $count += $retval;

            ## Because we read the rotated file, we want to slurp in the entire current file
            $offset = 0;

            ## Make sure we store our new modification time for the rotated file
            $opt{newrotatefilemodtime}{$filename} = $remoterotatedfileinfo->{modification};
        }

        ## Double check if we need the offset to be zero
        if ($filesize < $offset) {
            $offset = 0;
        }

        ## Read the current file.
        my $slurpsize = $arg{debug_partial_read} || $filesize;
        my $retval = slurp_via_pg_read_file($service, $newfilename, $offset, $slurpsize, \%pidline, $filename, $lfileinfo);
        return -7 if $retval < 0;
        $count += $retval;

        $newoffset = $filesize;

    } ## end SERVICE file mode
    else {

        ## Make sure the file exists and is readable
        if (! -e $filename) {
            $arg{quiet} or warn qq{WARNING! Skipping non-existent file "$filename"\n};
            return -5;
        }
        if (! -r $filename) {
            $arg{quiet} or warn qq{WARNING! Skipping unreadable file "$filename"\n};
            return -6;
        }

        ## Figure out where in the file we want to start scanning from
        my $size = -s $filename;
        my $offset = 0;
        my $maxsize = $opt{maxsize} ? $opt{maxsize} : $arg{maxsize};

        ## Is the offset significant?
        if (!$arg{file}[0]               ## ...not if we passed in filenames manually
                and $lastfile eq $filename   ## ...not if this is not the same file we got the offset for last time
        ) {
            ## Allow the offset to equal the size via --reset
            if ($arg{reset}) {
                $offset = $size;
                $arg{verbose} and warn "  Resetting offset to $offset\n";
            }
            ## Allow the offset to be changed on the command line
            elsif ($arg{offset} != -1) {
                if ($arg{offset} >= 0) {
                    $offset = $arg{offset};
                }
                elsif ($arg{offset} < -1) {
                    $offset = $size + $arg{offset};
                    $offset = 0 if $offset < 0;
                }
            }
            else {
                $offset = $lfileinfo->{offset} || 0;
            }
        }

        my $psize = pretty_number($size);
        my $pmaxs = pretty_number($maxsize);
        my $poffset = pretty_number($offset);
        $arg{verbose} and warn "  File: $filename Offset: $poffset Size: $psize Maxsize: $pmaxs\n";

        ## The file may have shrunk due to a logrotate
        if ($offset > $size) {
            $arg{verbose} and warn "  File has shrunk - resetting offset to 0\n";
            $offset = 0;
        }

        ## If the offset is equal to the size, we're done!
        ## Store the offset if it is truly new and significant
        if ($offset >= $size) {
            $offset = $size;
            if ($lfileinfo->{offset} != $offset) {
                $opt{newoffset}{$filename} = $offset;
            }
            return 0;
        }

        ## Store the original offset
        my $original_offset = $offset;

        ## This can happen quite a bit on busy files!
        if ($maxsize and ($size - $offset > $maxsize) and $arg{offset} < 0) {
            $arg{quiet} or warn "  SIZE TOO BIG (size=$size, offset=$offset): resetting to last $maxsize bytes\n";
            $toolarge{$filename} = qq{File "$filename" too large:\n  only read last $maxsize bytes (size=$size, offset=$offset)};
            $offset = $size - $maxsize;
        }

        open $fh, '<', $filename or die qq{Could not open "$filename": $!\n};

        ## Seek the right spot as needed
        if ($offset and $offset < $size) {

            ## Because we go back by 10 characters below, always offset at least 10
            $offset = 10 if $offset < 10;

            ## We go back 10 characters to get us before the newlines we (probably) ended with
            seek $fh, $offset-10, 0;

            ## If a manual rewind request has been given, process it (inverse)
            if ($arg{rewind}) {
                seek $fh, -$arg{rewind}, 1;
            }
        }

        ## Optionally figure out what approximate line we are on
        my $newlines = 0;
        if ($arg{find_line_number}) {
            my $pos = tell $fh;

            ## No sense in counting if we're at the start of the file!
            if ($pos > 1) {

                seek $fh, 0, 0;
                ## Need to sysread up to $pos
                my $blocksize = 100_000;
                my $current = 0;
                {
                    my $chunksize = $blocksize;
                    if ($current + $chunksize > $pos) {
                        $chunksize = $pos - $current;
                    }
                    my $foobar;
                    read $fh, $foobar, $chunksize;
                    ## How many newlines in there?
                    $newlines += $foobar =~ y/\n/\n/;
                    $current += $chunksize;
                    redo if $current < $pos;
                }

                ## Return to the original position
                seek $fh, 0, $pos;

            } ## end pos > 1

        } ## end find_line_number

        ## Get exclusion and inclusion regexes for this file
        ($exclude,$include,$exclude_prefix,$exclude_non_parsed,$bonus_include) = generate_regexes($filename);

        ## Discard the previous line if needed (we rewound by 10 characters above)
        $original_offset and <$fh>;

        my $current_pid_num = {};
        my ($retcount) = parse_filehandle($fh, $filename, \%pidline, $lfileinfo, $current_pid_num, $newlines);

        $count += $retcount;

        ## Get the new offset and store it
        seek $fh, 0, 1;
        $newoffset = tell $fh;

    } ## end NORMAL file mode

    close $fh if defined $fh;

    if ($lfileinfo->{offset} != $newoffset and $newoffset) {
        $opt{newoffset}{$filename} = $newoffset;
    }
    $lfileinfo->{modtime} ||= 0;
    if ($lfileinfo->{modtime} ne $newmodtime and $newmodtime) {
        $opt{newmodtime}{$filename} = $newmodtime;
    }

    ## Now add in any pids that have not been processed yet
    for my $pid (sort {
        $pidline{$a}{pgtime} cmp $pidline{$b}{pgtime}
        or
        $pidline{$a}{line} <=> $pidline{$b}{line}
    } keys %pidline) {
        $count += process_line($pidline{$pid}, 0, $filename);
    }

    if (!$count) {
        $arg{verbose} and warn "  No new lines found in file $filename\n";
    }
    else {
        $arg{verbose} and warn "  Lines found in $filename: $count\n";
    }

    if (exists $opt{total_non_parsed}) {
        $count -= $opt{total_non_parsed};
        $count++;
        delete $opt{total_non_parsed};
    }

    $opt{grand_total} += $count;

    return $count;

} ## end of parse_file


sub transform_filename {

    my $name = shift or die;

    ## Transform the file name if it contains escapes
    if ($name =~ /%/) {
        ## Allow moving back in time with the timewarp argument (defaults to 0)
        my @ltime = localtime(time + $arg{timewarp});
        $name = strftime($name, @ltime);
    }

    return $name;

} ## end of transform_filename


sub generate_regexes {

    ## Given a filename, generate exclusion and inclusion regexes for it

    ## Currently, all files get the same regex, so we cache it
    if (exists $opt{globalexcluderegex}) {
        return $opt{globalexcluderegex}, $opt{globalincluderegex}, $opt{globalexcludeprefixregex}, $opt{globalexcludenonparsedregex},$opt{globalbonusinclude};

    }

    ## Build an exclusion regex
    my $lexclude = '';
    if ($opt{no_inherit} =~ /exclude\b/i) {
        $opt{inherited_exclude} = [];
    }
    for my $ex (@{$opt{exclude}}, @{$opt{inherited_exclude}}) {
        $arg{debug} and warn "  Adding exclusion: $ex\n";
        my $regex = qr{$ex};
        $lexclude .= "$regex|";
    }
    $lexclude =~ s/\|$//;
    $arg{verbose} and $lexclude and warn "  Exclusion: $lexclude\n";

    ## Build an exclusion non-parsed regex
    my $lexclude_non_parsed = '';
    if ($opt{no_inherit} =~ /exclude_non_parsed/i) {
        $opt{inherited_exclude_non_parsed} = [];
    }
    for my $ex (@{$opt{exclude_non_parsed}}, @{$opt{inherited_exclude_non_parsed}}) {
        $arg{debug} and warn "  Adding exclusion_non_parsed: $ex\n";
        my $regex = qr{$ex};
        $lexclude_non_parsed .= "$regex|";
    }
    $lexclude_non_parsed =~ s/\|$//;
    $arg{verbose} and $lexclude_non_parsed and warn "  Exclusion_non_parsed: $lexclude_non_parsed\n";

    ## Build a prefix exclusion regex
    my $lexclude_prefix = '';
    if ($opt{no_inherit} =~ /exclude_prefix/i) {
        $opt{inherited_exclude_prefix} = [];
    }
    for my $ex (@{$opt{exclude_prefix}}, @{$opt{inherited_exclude_prefix}}) {
        $arg{debug} and warn "  Adding exclusion_prefix: $ex\n";
        my $regex = qr{$ex};
        $lexclude_prefix .= "$regex|";
    }
    $lexclude_prefix =~ s/\|$//;
    $arg{verbose} and $lexclude_prefix and warn "  Exclusion_prefix: $lexclude_prefix\n";

    ## Build an inclusion regex, and possibly some bonus inclusion items
    my $linclude = '';
    my $lbonus_include = [];
    if ($opt{no_inherit} =~ /include\b/i) {
        $opt{inherited_include} = [];
    }
    for my $in (@{$opt{include}}, @{$opt{inherited_include}}) {
        $arg{debug} and warn "  Adding inclusion: $in\n";
        my $lin = $in;
        if ($lin =~ s/(.+)([+\-](?:PREFIX|FILE)):\s*(.+)/$1/) {
            push @{ $lbonus_include } => [$1, $2, qr/$3/];
        }
        my $regex = qr{$lin};
        $linclude .= "$regex|";
    }
    $linclude =~ s/\|$//;
    $arg{verbose} and $linclude and warn "  Inclusion: $linclude\n";
    $opt{globalexcluderegex} = $lexclude;
    $opt{globalexcludeprefixregex} = $lexclude_prefix;
    $opt{globalexcludenonparsedregex} = $lexclude_non_parsed;
    $opt{globalincluderegex} = $linclude;
    $opt{globalbonusinclude} = $lbonus_include;

    return $lexclude, $linclude, $lexclude_prefix, $lexclude_non_parsed, $lbonus_include;

} ## end of generate_regexes


sub call_pg_stat_file {

    ## Get information about a remote log file via pg_stat_file()
    ## Arguments: two
    ## 1. Database service name
    ## 2. Remote file name
    ## Returns: hashref of information about the file, or an error string

    my ($dbservice, $dbfilename) = @_;

    ## We need a place to store any errors that come up
    my ($efh, $errorfile) = tempfile('tail_n_mail.pg_stat_file.errors.XXXXXXXX', SUFFIX => '.tnm', UNLINK => 1, TMPDIR => 1);

    ## Version 9.6 of Postgres has multiple -c options
    my $datestyle = q{"SET datestyle='ISO'"};
    my $statcommand = qq{"$app_string select * from pg_stat_file('$dbfilename')"};
    my $COM = qq{psql service="$dbservice" -AX -qt -x -c $datestyle -c $statcommand 2>$errorfile};
    $arg{debug} and warn "Trying: $COM\n";
    my @statfileinfo = qx/ $COM /;
    if (-s $errorfile) {
        seek $efh, 0, 0;
        my $errline = <$efh>;
        chomp $errline;
        ## This is handled elsewhere, so do not need to be noisy about it here:
        if ($errline !~ /could not stat file/) {
            $arg{verbose} and warn "Call to pg_stat_file failed for service $dbservice and file $dbfilename ($errline)\n";
        }
        return $errline;
    }
    close $efh;

    my %fileinfohash = map { chomp; split /\|/, $_, 2 } @statfileinfo;

    return \%fileinfohash;

} ## end of call_pg_stat_file


sub call_pg_ls_dir {

    ## Gather a directory listing via a service file entry and pg_ls_dir
    ## Arguments: two
    ## 1. Database service name
    ## 2. Remote directory name
    ## Returns: hashref with file names as the keys, and an empty hashref as the values

    my ($dbservice, $dbdirname) = @_;

    ## We need a place to store any errors that come up
    my ($efh, $errorfile) = tempfile('tail_n_mail.pg_ls_dir.errors.XXXXXXXX', SUFFIX => '.tnm', UNLINK => 1, TMPDIR => 1);

    my $COM = qq{psql service="$dbservice" -AX -qt -x -c "$app_string select pg_ls_dir('$dbdirname')" 2>$errorfile};
    $arg{debug} and warn "Trying: $COM\n";
    my @lsdirinfo = qx/ $COM /;
    if (-s $errorfile) {
        seek $efh, 0, 0;
        my $errline = <$efh>;
        chomp $errline;
        $arg{verbose} and warn "Call to pg_ls_dir failed for service $dbservice and dir $dbdirname ($errline)\n";
        return $errline;
    }
    close $efh;

    my %list_of_files = map { chomp; catfile($dbdirname,(split /\|/, $_, 2)[1]) => {} } grep { /\S/ } @lsdirinfo;

    return \%list_of_files;

} ## end of call_pg_ls_dir


sub get_latest_service_file {

    ## Figure out the most recent file inside a directory reached via a service file
    ## Arguments: two
    ## 1. Database service name
    ## 2. Remote file directory
    ## Returns: new file name

    my ($dbservice, $dbfiledir) = @_;

    my $file_list = call_pg_ls_dir( $dbservice, $dbfiledir );
    ref $file_list or die "Could not get a directory listing for service $dbservice";

    ## If these look like the default postgres log_line_format endings, we can sort it ourselves
    if ( ! grep { !/[0-9]_[0-9]+\.log/ } keys %$file_list) {
        $arg{debug} and warn "Doing a natural sort as all files look to be standard format\n";
        for my $first (reverse sort keys %$file_list) {
            return "$dbfiledir/$first";
        }
    }

    ## Get information about each one
    for my $filename (keys %$file_list) {
        my $remotefileinfo = call_pg_stat_file($dbservice, "$filename");
        ref $remotefileinfo or die "Failed to find information on file $filename\n";
        $file_list->{$filename}{mod} = $remotefileinfo->{modification}
            or die "No modification time found for $filename\n";
    }

    for my $first (sort { $file_list->{$b}{mod} cmp $file_list->{$a}{mod} } keys %$file_list) {
        return $first;
    }

    die "Could not determine a filename for service $dbservice\n";

} ## end of get_latest_service_file


sub slurp_via_pg_read_file {

    ## Grab the contents of a remote file via pg_read_file()
    ## Returns: count of matches, or -1 for pg_read_file failures
    ## Arguments: seven
    ## 1. Database service name
    ## 2. Remote file name
    ## 3. Starting offset
    ## 4. Requested bytes
    ## 5. PID line hashref
    ## 6. Final filename
    ## 7. Local file info

    my ($dbservice, $dbfilename, $fileoffset, $bytesneeded, $pidinfo, $filename, $lfileinfo) = @_;

    ## We need a place to store any errors that come up
    my ($efh, $errorfile) = tempfile('tail_n_mail.pg_read_file.errors.XXXXXXXX', SUFFIX => '.tnm', UNLINK => 1, TMPDIR => 1);

    ## We also want to store the output, rather than creating extra scalars
    my ($ofh, $outputfile) = tempfile('tail_n_mail.pg_read_file.output.XXXXXXXX', SUFFIX => '.tnm', UNLINK => 1, TMPDIR => 1);

    ## Because pg_read_file has an upper limit, we may need more than one run through
    my $sizeleft = $bytesneeded - $fileoffset;
    my $count = 0;
    my $current_pid_num = {};
    my $oldstring = '';
    my $round = 0;
    my $linenumber = 0;
    {

        $round++;

        ## We want to re-use the temp file each round. First, truncate it:
        truncate $ofh, 0;

        ## If there was a previous round, we need to add the partial line to this round.
        ## We chomp and check length because pg_read_file always adds a newline
        ## In the case where we stopped at the exact end of a line, it still adds a newline
        chomp $oldstring;
        if (length $oldstring) {
            seek $ofh, 0, 0;
            print {$ofh} $oldstring;
        }

        ## We want to grab as much as we can this round, up to the hard-coded limit above
        my $bytes = $sizeleft > $arg{pg_read_file_limit} ? $arg{pg_read_file_limit} : $sizeleft;
        return 0 if $bytes < 0;
        $arg{debug} and warn "Round $round: offset $fileoffset, left to read is $sizeleft, bytes is $bytes\n";

        ## Note that we append the output, due to the chance of $oldstring above
        my $SQL = "$app_string select * from pg_read_file('$dbfilename',$fileoffset,$bytes)";
        my $COM = qq{psql service="$dbservice" -AX -qt -c "$SQL" 2>$errorfile >>$outputfile};
        $arg{debug} and warn "Trying: $COM\n";
        system $COM;

        if (-s $errorfile) {
            seek $efh, 0, 0;
            my $errline = <$efh>;
            $arg{debug} and warn "Call to pg_read_file($dbfilename,$fileoffset,$bytes) failed for service $dbservice: $errline";

            ## If we had a conflict, try again after a short sleep
            if ($errline =~ /due to conflict with recovery/) {
                sleep 30;
            }
            ## If this was a UTF-8 error involving null bytes, try and filter them out
            elsif ($errline =~ /\b0x00\b/) {
                $SQL = "$app_string select * from regexp_replace(encode(pg_read_binary_file('$dbfilename',$fileoffset,$bytes), 'escape'), '\\\\000', '~', 'g')";
            }
            ## For everything else, we give up right away
            else {
                warn "Call to pg_read_file($dbfilename,$fileoffset,$bytes) failed for service $dbservice: $errline";
                return -1;
            }

            $COM = qq{psql service="$dbservice" -AX -qt -c "$SQL" 2>$errorfile >>$outputfile};
            $arg{debug} and warn "Trying: $COM\n";
            system $COM;

            if (-s $errorfile) {
                seek $efh, 0, 0;
                $errline = <$efh>;
                warn "Modified call to pg_read_file($dbfilename) failed for service $dbservice: $errline";
                return -1;
            }
        }

        ## Pass the temp filehandle directly to parse_filehandle
        seek $ofh, 0, 0;
        my ($retcount, $retlines, $retval) = parse_filehandle($ofh, $filename, $pidinfo, $lfileinfo, $current_pid_num, $linenumber);
        $count += $retcount;

        return $count if $arg{debug_partial_matches} and $count > $arg{debug_partial_matches};

        $sizeleft -= $bytes;
        $fileoffset += $bytes;

        last if !$sizeleft;

        ## Store the final bit that did not make up a full line
        $oldstring = $retval;

        ## Update our line count as well
        $linenumber = $retlines;

        redo;
    }

    close $efh;

    return $count;

} ## end of slurp_via_pg_read_file


sub process_line {

    ## We've got a complete statement, so do something with it!
    ## If it matches, we'll either put into %find directly, or store in %similar

    my ($info,$line,$filename,$forcematch,$special) = @_;

    ## The final string
    $string = '';
    ## The prefix
    my $pgprefix = '';
    ## The timestamp
    $time = '';

    if (ref $info eq 'HASH') {
        $pgprefix = $info->{pgprefix} if exists $info->{pgprefix};
        $time = $info->{pgtime} if exists $info->{pgtime};
        if (exists $info->{rawstring}) {
            $string = $info->{rawstring};
        }
        else {
            for my $l (sort {$a<=>$b} keys %{$info->{string}}) {
                ## Some Postgres/syslog combos produce ugly output
                $info->{string}{$l} =~ s/^(?:\s*#011\s*)+//o;
                $string .= ' '.$info->{string}{$l};
            }
        }
        $line = $info->{line};
    }
    else {
        $string = $info;
    }

    ## Non-parsed lines are grouped together by filename
    if (defined $special and $special eq 'nonparsed') {
        my $lookup = "nonparsed $filename";
        if (!exists $similar{$lookup}) {
            $similar{$lookup}{earliest} = $similar{$lookup}{latest} =
                {
                    filename => $filename,
                    line     => $line,
                    pgprefix => $pgprefix,
                    time     => $time,
                    npstring => $string,
                };
            $similar{$lookup}{count} = 1; ## Stays at 1

        }
        else {
            my $oldstring = $similar{$lookup}{latest}{npstring};
            ## This becomes the new latest one
            $similar{$lookup}{latest} =
                {
                    filename => $filename,
                    pgprefix => $pgprefix,
                    line     => $line,
                    time     => $time,
                    npstring => "$oldstring\n$string",
                };
        }

        ## We want to count all non-parsed lines as a single "hit"
        $opt{total_non_parsed}++;

        return 1;
    }

    ## Strip out leading whitespace
    $string =~ s/^\s+//o;

    ## Save the raw version
    my $rawstring = $string;

    ## Track durations
    my $thisduration = 0;

    ## Special handling for forced checks, e.g. OS errors
    if (defined $forcematch) {

        $pgprefix = '?';

        ## Bail if it matches the exclusion non-parsed regex
        return 0 if $exclude_non_parsed and $string =~ $exclude_non_parsed;

        goto PGPREFIX;
    }
    elsif ($arg{type} eq 'duration') {
        return 0 if $string !~ s/^\s*LOG:  duration: ([0-9]+\.[0-9]+) ms\s+//;
        $thisduration = $1;
        $string =~ s/^statement: //;
    }
    else {

        ## Bail if it does not match the file-specific regex
        my $filematch = 0;
        if ($opt{include_via_file}) {
            for my $in (@{$opt{include_via_file}}) {
                $in =~ m{(.+?)\s*~\s*(.+)} or die "Invalid INCLUDE_VIA_FILE entry: $in\n";
                my ($stringregex, $fileregex) = ($1,$2);
                $filematch = 1 if $filename =~ /$fileregex/ and $string =~ /$stringregex/;
            }
        }

        ## Bail if it does not match the inclusion regex
        return 0 if !$filematch and $include and $string !~ $include;

        ## Bail if it matches the exclusion regex
        return 0 if $exclude and $string =~ $exclude;

        ## Bail if it matches the prefix exclusion regex
        return 0 if $exclude_prefix and $pgprefix =~ $exclude_prefix;

        ## Bail if it matches the exclusion regex for this file
        if ($opt{exclude_via_file}) {
            for my $ex (@{$opt{exclude_via_file}}) {
                $ex =~ m{(.+?)\s*~\s*(.+)} or die "Invalid EXCLUDE_VIA_FILE entry: $ex\n";
                my ($stringregex, $fileregex) = ($1,$2);
                return 0 if $filename =~ /$fileregex/ and $string =~ /$stringregex/;
            }
        }

        ## Do the bonus regex checks
        for my $bonus (@$bonus_include) {
            if ($string =~ $bonus->[0]) {
                ## Bail if we matched, but we want to exclude based on the prefix
                return 0 if $bonus->[1] eq '-PREFIX' and $pgprefix =~ $bonus->[2];
                ## Bail if we matched, but we want to include based on the prefix
                return 0 if $bonus->[1] eq '+PREFIX' and $pgprefix !~ $bonus->[2];
                ## Bail if we matched, but we want to exclude based on the filename
                return 0 if $bonus->[1] eq '-FILE' and $filename =~ $bonus->[2];
                ## Bail if we matched, but we want to include based on the filename
                return 0 if $bonus->[1] eq '+FILE' and $filename !~ $bonus->[2];
            }
        }

    }


    $arg{debug} > 2 and warn "MATCH at line $line of $filename\n";

    ## Special handling for errant apps
    $string =~ s/(dsn was:.+ password=)\S+/$1\[elided\]/;

    ## Force newlines to a single line
    $string =~ s/\n/\\n/go;

    ## Compress all whitespace
    $string =~ s/\s+/ /go;

    ## Strip leading whitespace
    $string =~ s/^\s+//o;

    ## Replace EXECUTE foobar(x,y,z...) with dirt-simple param replacement
    $string =~ s{(STATEMENT: EXECUTE \w.+?)\((.+)\)}{
        my ($name,$args)=($1,$2);
        $args = join ',' => map { '?' } split /,/ => $args;
        "$1 ($args)"
    }ge;

    ## PgBouncer logs are fairly simple
    if ($arg{pglog} eq 'pgbouncer') {
        ## WARNING C-0x2ae3c50: myserver/alice@unix(18550):6432 Taking connection from reserve_pool<<
        ## WARNING S-0x10d6320: myserver/alice@123.45.67.80:5432 got packet 'E' from server when not linked
        $string =~ s/(\w+) [A-Z]\-0x.+?: /$1 /;
        $string =~ s/\([0-9]+\):/:/;
        goto PGPREFIX;
    }

    ## Reassign rawstring
    $rawstring = $string;

    ## Apply any custom flattening
    for my $item (@{ $opt{flatten} }) {
        if ($item !~ m{(.+)/(.+)}) {
            die "Flatten regex must be of format foo/bar\n";
        }
        my ($leftreg,$rightreg) = ($1,$2);
        $string =~ s/$leftreg/$rightreg/g;
    }

    ## If not in Postgres mode, we avoid all the mangling below
    if (! $pgmode) {
        $pgprefix = '?';
        goto PGPREFIX unless $arg{pgflatten};
    }

    ## For tempfiles, strip out the size information and store it
    my $tempfilesize = 0;
    if ($arg{type} eq 'tempfile') {
        if ($string =~ s/LOG: temporary file:.+?size ([0-9]+)\s*//o) {
            $tempfilesize = $1;
        }
        else {
            ## If we cannot figure out a size, skip this line
            return 1;
        }
        $string =~ s/^\s*STATEMENT:\s*//o;
    }

    ## Make some adjustments to attempt to compress similar entries
    if ($arg{flatten}) {

        ## Flatten array literals
        $string =~ s/'\{.+?\}'/'?'/g;

        ## Any error ending in a specific character
        $string =~ s{( at character) [0-9]+ (STATEMENT: )}{$1 ? $2};

        $string =~ s/'.+?'::(TEXT|VARCHAR|JSON|JSONB)/'foo'::$1/g;
        ## SELECT func(arg1,arg2,...) or SELECT * FROM func(arg1,arg2,...) or SELECT foo, bar FROM func(arg1,arg2,..) replacement
        $string =~ s{(SELECT\s*(?:\*\s+FROM\s*)?(?:[\w ,]+)?[\w\.]+\s*\()([^*].*?)\)(?!['"])}{
            my ($select,$args) = ($1,$2);
            my @arg;
            for my $arg (split /,/ => $args) {
                $arg =~ s/^\s*(.+?)\s*$/$1/;
                $arg = '?' if $arg !~ /^\$[0-9]/;
                push @arg => $arg;
            }
            "$select" . (join ',' => @arg) . ')';
        }geix;

        ## Flatten simple LIKE and ILIKE clauses
        $string =~ s{\b(I?LIKE)\s+'.*?'}{$1 '?'}gi;

        my $thisletter = '';

        $string =~ s{(VALUES|REPLACE)\s*\((.+)\)}{ ## For emacs: ()()()
            my ($sword,$list) = ($1,$2);
            my @word = split(//, $list);
            my $numitems = 0;
            my $status = 'start';
            my @dollar;

          F: for (my $x = 0; $x <= $#word; $x++) {

                $thisletter = $word[$x];
                if ($status eq 'start') {

                    ## Ignore white space and commas
                    if ($thisletter eq ' ' or $thisletter eq '    ' or $thisletter eq ',') {
                        next F;
                    }

                    $numitems++;
                    ## Is this a normal quoted string?
                    if ($thisletter eq q{'}) {
                        $status = 'inquote';
                        next F;
                    }
                    ## Perhaps E'' quoting?
                    if ($thisletter eq 'E') {
                        if (defined $word[$x+1] and $word[$x+1] ne q{'}) {
                            ## So weird we'll just pass it through
                            $status = 'fail';
                            last F;
                        }
                        $x++;
                        $status = 'inquote';
                        next F;
                    }
                    ## Dollar quoting
                    if ($thisletter eq '$') {
                        undef @dollar;
                        {
                            push @dollar => $word[$x++];
                            ## Give up if we don't find a matching dollar
                            if ($x > $#word) {
                                $status = 'fail';
                                last F;
                            }
                            if ($thisletter eq '$') {
                                $status = 'dollar';
                                next F;
                            }
                            redo;
                        }
                    }
                    ## Must be a literal
                    $status = 'literal';
                    next F;
                } ## end status 'start'

                if ($status eq 'literal') {

                    ## May be the end of the whole section
                    if ($thisletter eq ';') {
                        $sword .= "(?);";
                        $numitems = 0;

                        ## Grab everything forward from this point
                        my $newlist = substr($list,$x+1);

                        if ($newlist =~ m{(.+?(?:VALUES|REPLACE))\s*\(}io) {
                            $sword .= $1;
                            $x += length $1;
                        }

                        $status = 'start';
                        next F;
                    }

                    ## Almost always numbers. Just go until a comma
                    if ($thisletter eq ',') {
                        $status = 'start';
                    }
                    next F;
                }

                if ($status eq 'inquote') {
                    ## The only way out is an unescaped single quote
                    if ($thisletter eq q{'}) {
                        next F if $word[$x-1] eq '\\';
                        if (defined $word[$x+1] and $word[$x+1] eq q{'}) {
                            $x++;
                            next F;
                        }
                        $status = 'start';
                    }
                    next F;
                }

                if ($status eq 'dollar') {
                    ## Only way out is a matching dollar escape
                    if ($thisletter eq '$') {
                        ## Possibility
                        my $oldpos = $x++;
                        for (my $y=0; $y <= $#dollar; $y++, $x++) {
                            if ($dollar[$y] ne $thisletter) {
                                ## Tricked us - reset to next position
                                $x = $oldpos;
                                next F;
                            }
                        }
                        ## Got a match!
                        $x++;
                        $status = 'start';
                        next F;
                    }
                }

            } ## end each letter (F)

            if ($status eq 'fail') {
                "$sword ($list)";
            }
            else {
                "$sword (?)";
            }
        }geix;
        $string =~ s{(\bWHERE\s+\w+\s*=\s*)[0-9]+}{$1?}gio;
        $string =~ s{(\bWHERE\s+\w+[\.\w]*\s+IN\s*\((?!\s*SELECT))[^)]+\)}{$1?)}gio;
        $string =~ s{(\bWHERE\s+\w+[\.\w]*\s*=\s*)'.+?'}{$1'?')}gio;
        $string =~ s{(\bWHERE\s+\w+[\.\w]*\s*=\s*)[0-9]+}{$1'?')}gio;
        $string =~ s{(,\s*)'.+?'(\s*AS\s*\w+\b)}{$1'?'$2)}gio;
        $string =~ s{\(\s+}{(}go;

        $string =~ s{ for PID [0-9]+}{ for PID ?}g;

        my $UNIQSTRING = "TAILNMAIL$VERSION";
        if ($string =~ s{(UPDATE\s+[\w\.]+\s+SET\s+)(.+?)(\s+WHERE)}{${1}$UNIQSTRING${3}}) {
            my $sets = $2;
            $sets =~ s{([\w\.]+\s*=\s*)[0-9]+}{$1?}g;
            $sets =~ s{([\w\.]+\s*=\s*)'[^']*?'}{$1?}g;
            $string =~ s/$UNIQSTRING/$sets/e;
        }

        $string =~ s/(invalid byte sequence for encoding "UTF8": 0x)[a-f0-9]+/$1????/o;
        $string =~ s{(\(simple_geom,)'.+?'}{$1'???'}gio;
        $string =~ s{(DETAIL: Key \(.+?\))=\(.+?\)}{$1=(?)}go;
        $string =~ s{Failed on request of size [0-9]+}{Failed on request of size ?}go;
        $string =~ s{ARRAY\[.+?\]}{ARRAY[?]}go;
        $string =~ s{(invalid input syntax for [\w ]+): ".*?"(?: at character [0-9]+)*}{$1: "?"}o;
        $string =~ s{value ".+?" (is out of range for type)}{value "?" $1}o;
        $string =~ s{(field value out of range): ".+?" at character [0-9]+}{$1: "?" at character ?}g;
        $string =~ s{(Failing row contains) \(.+?\)\.}{$1 \(?\)\.}go;

        ## Sometimes FATAL errors get prepended with IP and port
        $string =~ s{\([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\([0-9]+\)\) FATAL}{(?) FATAL};

        ## pg_hba.conf errors from similar hosts can get combined
        ## e.g. Connection matched pg_hba.conf line 123: "hostssl all read,write 123.45.66.77/32 md5"
        $string =~ s{(matched pg_hba.conf line )[0-9]+:(.+?) [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+}{$1 ?:$2 ?}go;

        ## Combine similar login attemtps but leave the host as unique
        $string =~ s{(no pg_hba.conf entry for host ".+?", user) ".+?"(, database) ".+?"}{$1 ? $2 ?}g;

        ## WAL segment errors can be combined
        ## e.g. ERROR: requested WAL segment 0000000300002C3600000043 has already been removed
        $string =~ s{(requested WAL segment) [A-F0-9]+}{$1 ?}go;

        ## Syntax error at a specific character
        $string =~ s{(syntax error at or near) "\w+" at character [0-9]+}{$1 "?" at character ?}o;

        ## Ambiguity at a specific character
        $string =~ s{(" is ambiguous at character )[0-9]+}{$1 ?}o;

        ## Missing alias at a specific character
        $string =~ s{( must have an alias at character )[0-9]+}{$1 ?}o;

        ## Case of hard-coded numbers at the start of an inner SELECT
        $string =~ s{(\bSELECT\s*)[0-9]+,\s*[0-9]+,}{$1 ?,?,}gio;

        ## Some PostGIS functions
        $string =~ s{(\bST_GeomFromText\(').*?',[0-9]+\)}{$1(?,?)}gio;

        ## Declaring a named cursor
        $string =~ s{\b(DECLARE\s*(\S+)\s*CURSOR\b)}{DECLARE ? CURSOR}gio;

        ## Simple numbers after an AND
        $string =~ s{(\s+AND\s+\w+)\s*=[0-9]+(\b)}{$1=?$2}gio;

        ## Simple numbers after a SELECT
        $string =~ s{(\bSELECT\s+)[0-9]+(\b)}{$1?$2}gio;

        ## Raw number surrounded by commas
        $string =~ s{(,\s*)[0-9]+(\s*,)}{$1?$2}go;

        ## Single-quoted items after an AND
        $string =~ s{(\s+AND\s+\w+)\s*([<>=]+)\s*'.*?'}{$1 $2 ?}gio;

        ## Variable limit and offset clauses
        $string =~ s{\bLIMIT\s+([0-9]+)}{LIMIT ?}gi;
        $string =~ s{\bOFFSET\s+([0-9]+)}{OFFSET ?}gi;

        ## Simple castings
        $string =~ s{'.*?'::(BOOLEAN|BIGINT|INTEGER|INT|JSONB|NUMERIC|TIMESTAMP|TIMESTAMPTZ)}{?::$1}g;

        if ($string =~ /JSON/) {
            $string =~ s{("\w+"):"[[0-9]\.e\-]+"}{$1:"?"}g;
        }

        ## Timestamps as values
        $string =~ s{\s*=\s*'[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.[0-9][0-9][0-9][0-9][0-9][0-9]'}{=?}go;

        ## Precision timestamps anywhere
        $string =~ s{\b[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.[0-9]+\b}{?}g;

        ## Deadlocks - don't worry about process ids to help flattening
        if ($string =~ /deadlock detected/i) {
            $string =~ s{\b([Pp]rocess) [0-9]+}{$1 ?}go;
            $string =~ s{ transaction [0-9]+;}{ transaction ?;}go;
            $string =~ s{ locking tuple \([0-9]+,[0-9]+\)}{ locking tuple(?)}go;
            $string =~ s/'\{".+ (HINT: See server log)/$1/go;
        }

        ## Array literals
        $string =~ s{(malformed array literal:) ".+?"}{$1: ?}g;

    } ## end of flatten

    ## Format the final string (and rawstring) a little bit
    if ($arg{pretty_query}) {
        for my $word (qw/DETAIL HINT QUERY CONTEXT STATEMENT/) {
            $string =~ s/ *$word: /\n$word: /;
            $rawstring =~ s/ *$word: /\n$word: /;
        }
        $rawstring =~ s/^([A-Z]+: ) +/$1/gmo;
    }

    ## Special handling for tempfile mode
    if ($arg{type} eq 'tempfile') {
        ## We should know the temp file size by now
        ## We want to store up to four possible versions of a statement:
        ## Earliest, latest, smallest, largest

        ## The record we are going to store
        my $thisrecord = {
            filename => $filename,
            line     => $line,
            pgprefix => $pgprefix,
            time     => $time,
            filesize => $tempfilesize,
        };


        ## If we've not seen this before, simply create the structure and go
        if (! exists $similar{$string}) {

            ## Store all four versions as the same structure
            $similar{$string}{earliest} =
            $similar{$string}{latest} =
            $similar{$string}{smallest} =
            $similar{$string}{largest} =
                $thisrecord;

            ## Start counting how many times this statement appears
            $similar{$string}{count} = 1;

            ## Store the summary of all sizes so we can compute medians, stddev, etc.
            $similar{$string}{total} = $tempfilesize;

            return 1;

        } ## end if we have not seen this statement before in tempfile mode

        ## We have seen it before, so make some changes

        ## First, increment the count
        $similar{$string}{count}++;

        ## Update our total size
        $similar{$string}{total} += $tempfilesize;

        ## As files are read sequentially, this becomes the latest
        $similar{$string}{latest} = $thisrecord;

        ## If this size is larger than the current record holder, reassign the largest pointer
        if ($tempfilesize > $similar{$string}{largest}{filesize}) {
            $similar{$string}{largest} = $thisrecord;
        }
        ## If this size is smaller than the current record holder, reassign the smallest pointer
        if ($tempfilesize < $similar{$string}{smallest}{filesize}) {
            $similar{$string}{smallest} = $thisrecord;
        }

        return 1;

    } ## end of tempfile mode

    if ($arg{'canceled_autovac'}) {
        if ($string =~ /^\s*ERROR:\s+canceling autovacuum task\s*CONTEXT: automatic vacuum of table (.+)/) {
            if (1 == $arg{'canceled_autovac'}) {
                $fancyoutput{'canceled_autovac'}{$filename}{$1}++;
            }
            return 1;
        }
    }

    ## Special handling for "conflict with recovery" errors, as the STATEMENT is not too relevant
    if ($arg{'hide_conflict_error_details'} and $string =~ s/(ERROR: canceling statement due to conflict with recovery).+/$1/s) {
        $rawstring = '';
    }

  PGPREFIX:
    ## If we have a prefix, check for similar entries
    if (length $pgprefix) {

        ## We do not want to track empty lines (typically seen for non-Postgres logs only)
        return 0 if ! length $string;

        ## Seen this string before?

        if (exists $similar{$string}) {

            my $old = $similar{$string};

            ## We need to determine if this is a new variant of this string
            ## We only need to confirm this once, ever
            if (! exists $old->{foundvariant}) {
                if ($rawstring ne $old->{earliest}{rawstring}) {
                    $old->{foundvariant} = 1;
                }
            }

            ## We need to see if this becomes the winner of the "earliest" contest
            ## Switching files can cause this, but we assume entries inside the
            ## same file are always chronological
            if (! exists $old->{filecount}{$filename}) {
                ## We have one chance to claim the earliest prize from another file
                if ($old->{earliest}{filename} ne $filename and $time lt $old->{earliest}{time}) {
                    $old->{earliest} =
                        {
                            filename => $filename,
                            line     => $line,
                            pgprefix => $pgprefix,
                            time     => $time,
                            rawstring => $rawstring,
                        };
                }
            }

            ## We need to check the latest prize each time
            if ($old->{latest}{filename} eq $filename or $time gt $old->{latest}{time}) {
                $old->{latest} =
                    {
                        filename => $filename,
                        line     => $line,
                        pgprefix => $pgprefix,
                        time     => $time,
                        ## Do not need rawstring here
                    };
            }

        }
        else {
            ## Store as the earliest and latest version we've seen
            $similar{$string}{earliest} = $similar{$string}{latest} =
                {
                 filename => $filename,
                 line     => $line,
                 pgprefix => $pgprefix,
                 time     => $time,
                 rawstring => $rawstring,
                 };

        }

        ## Track the highest duration
        if ($arg{type} eq 'duration') {
            $similar{$string}{maxduration} = $thisduration if $thisduration > ($similar{$string}{maxduration}//0);
            $similar{$string}{minduration} = $thisduration if $thisduration < ($similar{$string}{minduration}//'infinity');
            $similar{$string}{totalduration} += $thisduration;
        }

        ## Increment the total count for this string
        $similar{$string}{count}++;

        ## Increment the count for this string in this file
        $similar{$string}{filecount}{$filename}++;

        ## At this point we have an earliest
        ## Store all filenames in case we need them
        if ($filename =~ /SERVICE=/) {
            if (! $similar{$string}{earliest}{allfileshash}{$filename}++) {
                push @{ $similar{$string}{earliest}{allfiles} } => $filename;
            }
        }
    }
    else {
        $find{$filename}{$line} = {
            string   => $string,
            count    => 1,
            line     => $line,
            filename => $filename,
        };
    }

    return 1;

} ## end of process_line


sub process_report {

    ## No files means nothing to do
    if (! @files_parsed) {
        $arg{quiet} or warn qq{No files were read in, exiting\n};
        exit 1;
    }

    ## How many files actually had things?
    my $matchfiles = 0;
    ## How many unique items?
    my $unique_matches = keys %similar;
    my $pretty_unique_matches = pretty_number($unique_matches);

    ## Which was the latest to contain something?
    my $last_file_parsed;
    for my $file (@files_parsed) {
        next if ! $file->[1];
        $matchfiles++;
        $last_file_parsed = $file->[0];
    }

    my $grand_total = $opt{grand_total};
    my $pretty_grand_total = pretty_number($grand_total);

    ## If not files matched, output the last one processed
    $last_file_parsed = $files_parsed[-1]->[0] if ! defined $last_file_parsed;

    ## Subject with replaced keywords:
    my $subject = $opt{mailsubject} || $DEFAULT_SUBJECT;
    $subject =~ s/FILE/$last_file_parsed/g;
    $subject =~ s/HOST/$hostname/g;
    if ($arg{tsepnosub} or $opt{tsepnosub}) {
        $subject =~ s/NUMBER/$grand_total/g;
        $subject =~ s/UNIQUE/$unique_matches/g;
    }
    else {
        $subject =~ s/NUMBER/$pretty_grand_total/g;
        $subject =~ s/UNIQUE/$pretty_unique_matches/g;
    }

    ## Store the header separate from the body for later size checking
    my @header;

    $arg{html} and push @header => 'Content-type: text/html';

    ## Discourage vacation programs from replying
    push @header => 'Auto-Submitted: auto-generated';
    push @header => 'Precedence: bulk';

    ## Some minor help with debugging
    push @header => "X-TNM-VERSION: $VERSION";

    ## Make sure we know when things are manually adjusted
    if ($arg{debug_partial_read}) {
        push @header => "X-WARNING: debug_partial_read was set to $arg{debug_partial_read}";
    }

    ## Sometimes we go dryrun for this section only!
    my $dryrun = $arg{dryrun};

    ## If our email field is all caps or has example.com, force to mostly dryrun mode
    if (! $dryrun) {
        for my $email (@{ $opt{email} }) {
            if ($email =~ /\@example.com/i) {
                $arg{verbose} and print "Switching to dry-run mode because email is 'example.com'\n";
                $dryrun = 2;
                last;
            }
            if ($email =~ /^[A-Z]+$/) { ## e.g. no '@' sign
                $arg{verbose} and print "Switching to dry-run mode because email is only all-caps\n";
                $dryrun = 2;
                last;
            }
        }
    }

    ## Allow no specific email for dryruns
    if (! exists $opt{email} or ! @{$opt{email}}) {
        if ($dryrun or $arg{nomail}) {
            push @{$opt{email}} => 'dryrun@example.com';
        }
    }

    ## Fill out the "To:" fields
    for my $email (@{$opt{email}}) {
        push @header => "To: $email";
    }
    if (! @{$opt{email}}) {
        die "Cannot send email without knowing who to send to!\n";
    }

    my $mailcom = $opt{mailcom} || $arg{mailcom};

    ## Custom From:
    my $from_addr = $opt{from} || '';
    if ($from_addr ne '') {
        push @header => "From: $from_addr";
        $mailcom .= " -f $from_addr";
    }
    ## End header section
    my @msg;

    my $tz = strftime('%Z', localtime());
    my $now = scalar localtime;
    push @msg => "Date: $now $tz";
    push @msg => "Host: $hostname";
    if ($arg{timewarp}) {
        push @msg => "Timewarp: $arg{timewarp}";
    }
    if ($arg{duration} >= 0) {
        push @msg => "Duration minimum: $arg{duration} ms";
    }
    if ($arg{tempfile} >= 0) {
        push @msg => "Minimum tempfile size: $arg{tempfile} bytes";
    }
    if ($arg{skippedfile}) {
        push @msg => 'Files skipped: ' . keys %{ $arg{skippedfile} };
        if ($arg{verbose}) {
            for my $file (sort keys %{ $arg{skippedfile} }) {
                push @msg => "Skipped file: $file";
            }
        }
    }

    if ($arg{type} eq 'normal') {
        push @msg => "Unique items: $pretty_unique_matches";
    }

    ## Attempt to line up all the SERVICE= entries
    my $maxservice = 0;
    my $allservice = 1;
    my %service_name;
    for my $file (@files_parsed) {
        $file->[2] = $file->[0];
        next if ! $file->[1];
        if ($file->[0] =~ /SERVICE=($servicenameregex)/) {
            $maxservice = length $1 if length $1 > $maxservice;
            $service_name{$1}++;
        }
        else {
            $allservice = 0;
        }
    }
    if ($maxservice) {
        for my $file (@files_parsed) {
            next if ! $file->[1];
            $file->[2] =~ s/(SERVICE=($servicenameregex))/$1 . ' 'x($maxservice - length $2)/e;
        }
    }

    ## If any service names are identical (e.g. different log files), do not show just the name
    $allservice = 0 if grep { $_ > 1 } values %service_name;

    ## Just in case anyone needs the old behavior:
    $allservice = 0 if $arg{'force-letter-output'};

    ## Setup fancy output
    my $letter = 0;
    my $maxcount = 1;
    my $maxname = 1;
    my $maxletter = 1;
    for my $file (@files_parsed) {

        ## Skip if no matches for this file
        next if ! $file->[1];

        ## Format the number of matches and track the string length
        $file->[1] = pretty_number($file->[1]);
        $maxcount = length $file->[1] if length $file->[1] > $maxcount;

        ## Track the longest file
        $maxname = length $file->[2] if length $file->[2] > $maxname;

        ## Assign a letter to each file, unless we are using all service files
        my $name;
        if ($allservice) {
            $name = ($file->[0] =~ /SERVICE=($servicenameregex)/) ? $1 : '?';
        }
        else {
            $name = chr(65+$letter);
            if ($letter >= 26) {
                $name = sprintf '%s%s',
                    chr(64+($letter/26)), chr(65+($letter%26));
            }
            $letter++;
        }

        $fab{$file->[0]} = $name;
        $maxletter = length $name if length $name > $maxletter;
    }

    ## If we parsed more than one file, label them now
    if ($matchfiles > 1) {
        push @msg => "Total matches: $pretty_grand_total";
        for my $file (@files_parsed) {
            next if ! $file->[1];
            my $name = $fab{$file->[0]};
            if ($allservice) {
                push @msg => sprintf 'Matches from %-*s %*s',
                    $maxname+1,
                    $file->[2] ? "$file->[2]:" : "$file->[0]:",
                    $maxcount,
                    $file->[1];
            }
            else {
                push @msg => sprintf 'Matches from %-*s %-*s %*s',
                    $maxletter + 2,
                    "[$name]",
                    $maxname+1,
                    $file->[2] ? "$file->[2]:" : "$file->[0]:",
                    $maxcount,
                    $file->[1];
            }
        }
    }
    elsif ($matchfiles) {
        push @msg => "Matches from $last_file_parsed: $pretty_grand_total";
    }

    for my $file (@files_parsed) {
        if (exists $toolarge{$file->[0]}) {
            push @msg => "$toolarge{$file->[0]}";
        }
    }

    if ($arg{type} eq 'duration' and $arg{duration_limit} and $grand_total > abs($arg{duration_limit})) {
        push @msg => 'Not showing all lines: duration limit is ' . abs($arg{duration_limit});
    }
    if ($arg{type} eq 'tempfile' and $arg{tempfile_limit} and $grand_total > $arg{tempfile_limit}) {
        push @msg => "Not showing all lines: tempfile limit is $arg{tempfile_limit}";
    }
    if ($arg{start_time}) {
        push @msg => "Start time: $arg{original_start_time}";
    }


    ## Note any special problems from file parsing
    for my $file (@problem_files) {
        push @msg => "$file";
    }

    ## Create the mail message
    my ($bigfh, $bigfile) = tempfile('tail_n_mail.message.XXXXXXXX', SUFFIX => '.tnm', TMPDIR => 1);

    ## The meat of the message: save to the temporary file
    lines_of_interest($bigfh, $matchfiles);
    print {$bigfh} "\n";

    ## Are we going to need to chunk it up?
    my $filesize = -s $bigfile;
    my $split = 0;
    if ($filesize > $arg{maxemailsize} and ! $dryrun) {
        $split = 1;
        $arg{verbose} and print qq{File $bigfile too big ($filesize > $arg{maxemailsize})\n};
    }

    my $emails = join ' ' => @{$opt{email}};

    ## Sanity check on number of loops below
    my $safety = 1;

    ## If chunking, which chunk are we currently on?
    my $chunk = 0;

    ## Where in the data file are we starting from?
    my $start_point = 0;
    ## Where in the data file are we going to? 0 means until the end
    my $stop_point = 0;

  LOOP: {

        ## If we are splitting, calculate the new start and stop points
        if ($split) {
            $chunk++;

            ## Start at the old stop point
            $start_point = $stop_point;
            ## Seek up to it, then walk backwards etc.

            seek $bigfh, $start_point + $arg{maxemailsize}, 0;
            my $firstpos = tell $bigfh;
            if ($firstpos >= $filesize) {
                ## We are done!
                $stop_point = 0;
                $split = 0;
            }
            else {
                ## Go backwards a few chunks at a time, see if we can find a good stop point
                ROUND: for my $round (1..10) {
                      seek $bigfh, $firstpos - ($round*5000), 0;

                      ## Only seek forward 10 lines
                      my $lines = 0;

                      while (<$bigfh>) {
                          ## Got a match? Rewind to just before the opening number
                          if ($_ =~ /(.*?)^\[[0-9]/ms) {
                              my $rewind = length($_) - length($1);
                              $stop_point = tell($bigfh) - $rewind;
                              seek $bigfh, $stop_point, 0;
                              if ($start_point >= $stop_point) {
                                  $stop_point = 0;
                                  $split = 0;
                              }
                              last ROUND;
                          }
                          last if $lines++ > 10;
                      }
                      $stop_point = 0;
                      $split = 0;
                  }
            }
        }

        ## Add the subject, adjusting it if needed
        my $newsubject = $subject;
        if ($chunk) {
            $newsubject = "[Chunk $chunk] $subject";
        }

        ## Prepend the header info to our new data file
        my ($efh, $emailfile) = tempfile('tail_n_mail.email.XXXXXXXX', SUFFIX => '.tnm', TMPDIR => 1);
        if ($dryrun) {
            close $efh or warn 'Could not close filehandle';
            $efh = \*STDOUT;
        }
        print {$efh} "Subject: $newsubject\n";
        for (@header) {
            print {$efh} "$_\n";
        }

        ## Stop headers, start the message
        print {$efh} "\n";

        $arg{html} and print {$efh} "<pre>\n";

        $chunk and print {$efh} "Message was split: this is chunk #$chunk\n";
        for (@msg) {
            if ($arg{html}) {
                s/&/&amp;/g;
                s/</&lt;/g;
                s/>/&gt;/g;
            }
            print {$efh} "$_\n";
        }
        ## Add a little space before the actual data
        print {$efh} "\n";

        ## Add some subset of the data file to our new temp file
        seek $bigfh, $start_point, 0;

        while (<$bigfh>) {
            if ($arg{html}) {
                s/&/&amp;/g;
                s/</&lt;/g;
                s/>/&gt;/g;
            }
            print {$efh} $_;
            next if ! $stop_point;
            last if tell $bigfh >= $stop_point;
        }

        ## Add any fancy output
        for my $fotype (sort keys %fancyoutput) {
            if ('canceled_autovac' eq $fotype) {
                print {$efh} "Number of times autovacuum was cancelled:\n";
                my $max_table_length = 1;
                for my $file (keys %{ $fancyoutput{$fotype} }) {
                    for my $tablename (keys %{ $fancyoutput{$fotype}{$file} }) {
                        $max_table_length = length $tablename if length $tablename > $max_table_length;
                    }
                }
                for my $file (sort keys %{ $fancyoutput{$fotype} }) {
                    for my $tablename (
                        sort { $fancyoutput{$fotype}{$file}{$b} <=> $fancyoutput{$fotype}{$file}{$a} or $a cmp $b }
                        keys %{$fancyoutput{$fotype}{$file}}) {
                        printf {$efh} "[%s] %-*s : %d\n", $fab{$file}, $max_table_length, $tablename, $fancyoutput{$fotype}{$file}{$tablename};
                    }
                }
                print {$efh} "\n\n";
            }
        }

        ## If we have a signature, add it
        if ($arg{mailsignature}) {
            $_ = $arg{mailsignature};
            if ($arg{html}) {
                s/&/&amp;/g;
                s/</&lt;/g;
                s/>/&gt;/g;
            }
            ## Caller's responsibility to add a "--" line
            print {$efh} $_;
        }

        $arg{html} and print {$efh} "</pre>\n";

        close $efh or warn qq{Could not close $emailfile: $!\n};

        $arg{verbose} and warn "  Sending mail to: $emails\n";
        my $COM = qq{$mailcom '$emails' < $emailfile};
        if ($dryrun or $arg{nomail}) {
            $arg{quiet} or warn "  DRYRUN: $COM\n";
        }
        else {
            my $mailmode = $arg{smtp} ? 'smtp' : $opt{mailmode} || $arg{mailmode};
            if ($arg{mailmode} eq 'sendmail') {
                system $COM;
            }
            elsif ($arg{mailmode} eq 'smtp') {
                send_smtp_email($from_addr, $emails, $newsubject, $emailfile);
            }
            else {
                die "Unknown mailmode: $mailmode\n";
            }
        }

        ## Remove our temp file
        unlink $emailfile;

        ## If we didn't split, we are done
        if (! $split) {
            ## Clean up the original data file and leave
            unlink $bigfile;
            return;
        }

        ## Sleep a little bit to not overwhelm the mail system
        sleep 1;

        ## In case of bugs or very large messages, set an upper limit on loops
        if ($safety++ > 5) {
            die qq{Too many loops, bailing out!\n};
        }

        redo;
    }

    ## Clean up the original data file
    unlink $bigfile;

    return;

} ## end of process_report


sub send_smtp_email {

    ## Send email via an authenticated SMTP connection

    ## For Windows, you will need:
    # perl 5.10
    # http://cpan.uwinnipeg.ca/PPMPackages/10xx/
    # ppm install Net_SSLeay.ppd
    # ppm install IO-Socket-SSL.ppd
    # ppm install Authen-SASL.ppd
    # ppm install Net-SMTP-SSL.ppd

    ## For non-Windows:
    # perl-Net-SMTP-SSL package
    # perl-Authen-SASL

    my ($from_addr,$emails,$tempfile) = @_;

    require Net::SMTP;

    ## Absorb any values set by rc files, and sanity check things
    my $mailserver = $opt{mailserver} || $arg{mailserver};
    if ($mailserver eq 'example.com') {
        die qq{When using smtp mode, you must specify a mailserver!\n};
    }
    my $mailuser = $opt{mailuser} || $arg{mailuser};
    if ($mailuser eq 'example') {
        die qq{When using smtp mode, you must specify a mailuser!\n};
    }
    my $mailpass = $opt{mailpass} || $arg{mailpass};
    if ($mailpass eq 'example') {
        die qq{When using smtp mode, you must specify a mailpass!\n};
    }
    my $mailport = $opt{mailport} || $arg{mailport};

    ## Attempt to connect to the server
    my $smtp;
    if (not $smtp = Net::SMTP->new(
        $mailserver,
        Port    => $mailport,
        Debug   => 0,
        Timeout => 30,
        SSL     => 1,
    )) {
        die qq{Failed to connect to mail server: $!};
    }

    ## Attempt to authenticate
    if (not $smtp->auth($mailuser, $mailpass)) {
        die 'Failed to authenticate to mail server: ' . $smtp->message;
    }

    ## Prepare to send the message
    $smtp->mail($from_addr) or die 'Failed to send mail (from): ' . $smtp->message;
    $smtp->to($emails)      or die 'Failed to send mail (to): '   . $smtp->message;
    $smtp->data()           or die 'Failed to send mail (data): ' . $smtp->message;
    ## Grab the lines from the tempfile and pipe it on to the server
    open my $fh, '<', $tempfile or die qq{Could not open "$tempfile": $!\n};
    while (<$fh>) {
        $smtp->datasend($_);
    }
    close $fh or warn qq{Could not close "$tempfile": $!\n};
    $smtp->dataend() or die 'Failed to send mail (dataend): ' . $smtp->message;
    $smtp->quit      or die 'Failed to send mail (quit): '    . $smtp->message;

    return;

} ## end of send_smtp_email


sub lines_of_interest {

    ## Given a file handle, print all our current lines to it

    my ($lfh,$matchfiles) = @_;

    my $oldselect = select $lfh;

    sub sortsub { ## no critic (ProhibitNestedSubs)

        my $sorttype = $opt{sortby} || $arg{sortby};

        my $aa = $similar{$a};
        my $bb = $similar{$b};

        if ($arg{type} eq 'duration') {

            return ($bb->{maxduration} <=> $aa->{maxduration}
                    || ($aa->{earliest}{time} cmp $bb->{earliest}{time}));

        }

        ## For tempfile, we want to sort by largest overall tempfile
        ## In a tie, we do the highest mean, count, filename, then line number!
        elsif ($arg{type} eq 'tempfile') {
            return ($bb->{largest}{filesize} <=> $aa->{largest}{filesize})
                    || $bb->{mean} <=> $aa->{mean}
                    || $bb->{count} <=> $aa->{count}
                    || ($fileorder{$aa->{filename}} <=> $fileorder{$bb->{filename}})
                    || ($aa->{line} <=> $bb->{line});
        }

        ## Special sorting for the display of means
        elsif ($arg{type} eq 'tempfilemean') {
            return ($bb->{mean} <=> $aa->{mean})
                    || ($fileorder{$aa->{filename}} <=> $fileorder{$bb->{filename}})
                    || ($aa->{line} <=> $bb->{line});
        }

        ## Special sorting for the display of temp files
        elsif ($arg{type} eq 'tempfiletotal') {
            return ($bb->{total} <=> $aa->{total})
                    || ($fileorder{$aa->{filename}} <=> $fileorder{$bb->{filename}})
                    || ($aa->{line} <=> $bb->{line});
        }

        elsif ($sorttype eq 'count') {
            return ($bb->{count} <=> $aa->{count})
                    || scalar keys %{ $aa->{filecount} } <=> scalar keys %{ $bb->{filecount} }
                    || ($aa->{earliest}{time} cmp $bb->{earliest}{time})
                    || ($aa->{earliest}{line} <=> $bb->{earliest}{line});
        }

        elsif ($sorttype eq 'date') {
            return ($fileorder{$aa->{filename}} <=> $fileorder{$bb->{filename}})
                || ($aa->{line} <=> $bb->{line});

        }

        return $aa <=> $bb;
    }

    my $count = 0;

    for my $string (sort sortsub keys %similar) {

        my $f = $similar{$string};

        $count++;

        last if $arg{showonly} and $count > $arg{showonly};

        ## Sometimes we don't want to show all the durations
        if ($arg{type} eq 'duration' and $arg{duration_limit}) {
            last if $count > abs $arg{duration_limit};
        }

        ## Exclude durations that are too short
        if ($arg{type} eq 'duration') {
            next if $f->{maxduration} < $arg{duration};
        }

        ## Sometimes we don't want to show all the tempfiles
        if ($arg{type} eq 'tempfile' and $arg{tempfile_limit}) {
            last if $count > $arg{tempfile_limit};
        }

        printf "\n%s", $arg{show_file_counter} ? "[$count] " : '';

        my $earliest = $f->{earliest};
        my $filename = $earliest->{filename};

        ## If only a single entry, simpler output
        if (exists $f->{latest}{npstring} or (1 == $f->{count} and $arg{hideflatten})) {
            if (exists $f->{latest}{npstring}) {
                if ($f->{earliest}{line} != $f->{latest}{line}) {
                    $f->{line} = "$f->{earliest}{line} - $f->{latest}{line}";
                }
                $earliest->{rawstring} = $f->{latest}{npstring};
            }

            if ($matchfiles > 1) {
                printf "From %s%s\n",
                    $fab{$filename},
                    $arg{find_line_number}
                       ? (sprintf ' (%s %s)', $earliest->{line} =~ / / ? 'lines' : 'line', $earliest->{line})
                       : '';
            }
            elsif ($arg{find_line_number}) {
                printf "(from %s %s)\n", $earliest->{line} =~ / / ? 'lines' : 'line', $earliest->{line};
            }
            else {
                print "\n";
            }

            ## If in tempfile mode, show the prettified information here
            if ($arg{type} eq 'tempfile') {
                printf "Temp file size: %s\n", pretty_size($f->{largest}{filesize});
            }

            ## If we are using prefixes, show it here
            if (exists $f->{earliest}{pgprefix} and length $f->{earliest}{pgprefix} and $f->{earliest}{pgprefix} ne '?') {
                print "$f->{earliest}{pgprefix}\n";
            }

            if ($arg{type} eq 'duration') {
                printf "Duration: %s ms\n", $f->{maxduration};
            }

            ## Show the actual string, not the flattened version (unless pgbouncer)
            my $nonparsed = exists $f->{latest}{npstring} ? 1 : 0;
            my $lstring = wrapline( ($arg{pglog} eq 'pgbouncer' ? $string : ($earliest->{rawstring} || $string) || ''), $nonparsed );
            print "$lstring\n";

            next;
        }

        my $perfile = '';
        if ($arg{show_file_numbers}) {
            if (1 < keys %{ $f->{filecount} }) {
                $perfile = sprintf ' (%s)',
                    join ', ' => map { pretty_number($f->{filecount}{$_}) }
                        sort { $f->{filecount}{$b} <=> $f->{filecount}{$a} or $fab{$a} cmp $fab{$b} } keys %{ $f->{filecount} };
            }
        }

        ## More than one entry means we have an earliest and latest to look at
        my $latest = $f->{latest};
        my $pcount = pretty_number($f->{count});

        if ($arg{find_line_number}) {
            $earliest->{line} = pretty_number($earliest->{line});
            $latest->{line} = pretty_number($latest->{line});
        }

        ## How many files did this span?
        my $filecount = scalar keys %{ $f->{filecount} // {} };

        my $filelist = join ', ' => map { $fab{$_} } sort {
            ($arg{show_file_numbers} ? ($f->{filecount}{$b} cmp $f->{filecount}{$a}) : 0)
                or $fab{$a} cmp $fab{$b}
            }
            keys %{ $f->{filecount} };

        print "From $filelist" if $matchfiles > 1;
        if ($arg{find_line_number}) {
            if (1 == $filecount) {
                ## All matches came from a single file
                printf '(between lines %s and %s, occurs %d times)',
                    $earliest->{line}, $latest->{line}, $pcount;
            }
            else {
                ## Matches came from multiple files
                printf '(between lines %s of %s and %s of %s, occurs %d times)',
                    $earliest->{line}, $fab{ $earliest->{file} },
                    $latest->{line}, $fab{ $latest->{file} },
                        $pcount;
            }
        }
        else {
            print "  Count: $pcount";
        }

        print "$perfile\n";

        if ($arg{type} eq 'tempfile') {

            ## If there was more than one, show some summary information
            if ($f->{count} > 1) {
                printf "Arithmetic mean is %s, total size is %s\n",
                    pretty_size($f->{mean}), pretty_size($f->{total});
            }

            ## Show the exact size, or the smallest and largest if both available
            if ($f->{smallest}{filesize} == $f->{largest}{filesize}) {
                printf "Temp file size: %s\n", pretty_size($f->{largest}{filesize});
            }
            else {
                ## Show the smallest and the largest temp files used for this statement
                ## Show the prefix (e.g. timestamp) when it occurred if available

                my $s = $f->{smallest};
                printf "Smallest temp file size: %s%s\n",
                    pretty_size($s->{filesize}),
                    (exists $s->{pgprefix} and $s->{pgprefix} ne '?') ? " ($s->{pgprefix})" : '';

                my $l = $f->{largest};
                printf "Largest temp file size: %s%s\n",
                    pretty_size($l->{filesize}),
                    (exists $l->{pgprefix} and $l->{pgprefix} ne '?') ? " ($l->{pgprefix})" : '';
            }
        }

        if ($arg{type} eq 'duration') {
            printf "Maximum duration: %s ms\n", $f->{maxduration};
            printf "Minimum duration: %s ms\n", $f->{minduration};
            printf "Average duration: %.3f ms\n", $f->{totalduration} / $f->{count};
        }

        ## If we have prefixes available, show those
        my $estring = exists $f->{foundvariant} ? $string : $earliest->{rawstring};
        if (exists $earliest->{pgprefix}) {
            if ($earliest->{pgprefix} ne '?') { ## Skip direct lines
                printf "First: %s%s\nLast:  %s%s\n",
                    1 == $filecount ? '' : "[$fab{$earliest->{filename}}] ",
                    $earliest->{pgprefix},
                    1 == $filecount ? '' : "[$fab{$latest->{filename}}] ",
                    $latest->{pgprefix};
            }
            $estring =~ s/^\s+//o;
            print wrapline($estring);
            print "\n";
        }
        else {
            print " Earliest and latest:\n";
            print wrapline($estring);
            print "\n";
            print wrapline($latest->{string});
            print "\n";
        }

        ## Show the first actual error if we've flattened things out (unless pgbouncer)
        if (exists $f->{foundvariant} and $arg{pglog} ne 'pgbouncer' and length $earliest->{rawstring}) {
            print "-\n";
            print wrapline($earliest->{rawstring});
            print "\n";
        }

    } ## end each item

    ## Flatten the items for ease of sorting
    my @sorted;
    for my $f (keys %find) {
        for my $l (keys %{$find{$f}}) {
            push @sorted => $find{$f}{$l};
        }
    }

    ## If we are in tempfile mode, perform some statistics
    if ($arg{type} eq 'tempfile') {

        for my $row (@sorted) {
            $row->{mean} = int ($row->{total} / $row->{count});
            ## Mode is meaningless, median too hard to compute
        }

        ## We want to show the top X means

        ## Assign our numbers so we can display the list of means
        my $tempcount = 0;
        for my $f (sort sortsub @sorted) {
            $tempcount++;
            $f->{displaycount} = $tempcount;
        }

        ## Gather the means
        my @mean;
        $arg{type} = 'tempfilemean'; ## Trickery
        my $maxmean = 0;
        my $meancount = 0;
        for my $f (sort sortsub @sorted) {
            $meancount++;
            my $item = sprintf '(item %d, count is %d)', $f->{displaycount}, $f->{count};
            push @mean => sprintf '%10s %-22s', pretty_size($f->{mean},1), $item;
            $maxmean = $f->{displaycount} if $f->{displaycount} > $maxmean;
            last if $arg{tempfile_limit} and $meancount >= $arg{tempfile_limit};
        }

        ## Gather the totaltemp
        my @totaltemp;
        $arg{type} = 'tempfiletotal';
        my $maxtotal = 0;
        my $totalcount = 0;
        for my $f (sort sortsub @sorted) {
            $totalcount++;
            my $item = sprintf '(item %d, count is %d)', $f->{displaycount}, $f->{count};
            push @totaltemp => sprintf '%10s %-22s', pretty_size($f->{total},1), $item;
            $maxtotal = $f->{displaycount} if $f->{displaycount} > $maxtotal;
            last if $arg{tempfile_limit} and $totalcount >= $arg{tempfile_limit};
        }
        $arg{type} = 'tempfile';

        ## Print out both the mean and the total
        print "  Top items by arithmetic mean    |   Top items by total size\n";
        print "----------------------------------+-------------------------------\n";
        $tempcount = 0;
        {
            last if ! defined $mean[$tempcount] and ! defined $totaltemp[$tempcount];
            printf '%-s |', defined $mean[$tempcount] ? $mean[$tempcount] : '';
            printf "%s\n", defined $totaltemp[$tempcount] ? $totaltemp[$tempcount] : '';
            $tempcount++;
            redo;
        }

        ## Set a new tempfile_limit based on how many mean entries we found above
        if ($maxmean > $arg{tempfile_limit}) {
            $arg{tempfile_limit} = $maxmean;
        }
        if ($maxtotal > $arg{tempfile_limit}) {
            $arg{tempfile_limit} = $maxtotal;
        }

    } ## end of tempfile mode

    select $oldselect;

    return;

} ## end of lines_of_interest


sub wrapline {

    ## Truncate lines that are too long
    ## Wrap long lines to make SMTP servers happy

    my $line = shift;
    my $nonparsed = shift || 0;

    my $len = length $line;
    my $olen = $len;
    my $olinelength = $line =~ y/\n//;
    my $waschopped = 0;
    my $maxsize = defined $opt{statement_size}
        ? $opt{statement_size}
        : $arg{statement_size};

    if ($nonparsed) {
        $maxsize = defined $opt{nonparsed_statement_size}
        ? $opt{nonparsed_statement_size}
        : $arg{nonparsed_statement_size};
    }

    if ($maxsize and $len > $maxsize) {
        $line = substr($line,0,$maxsize);
        $waschopped = 1;
        $len = $maxsize;
    }

    if ($len >= $arg{wraplimit}) {
        $line =~ s{(.{$arg{wraplimit}})}{$1\n}g;
    }

    if ($waschopped) {
        $line .= sprintf "\n[LINE TRUNCATED, original was %s characters%s long]",
            pretty_number($olen),
            ($olinelength > 2 ? ( ' and ' . pretty_number($olinelength) . ' lines' ) : '');
    }

    return $line;

} ## end of wrapline


sub final_cleanup {

    $arg{debug} and warn "  Performing final cleanup\n";

    ## Need to walk through and see if anything has changed so we can rewrite the config
    ## For the moment, that only means the offset and the lastfile

    ## Have we got new lastfiles, offsets, modtimes, or rotatefilemodtimes?
    for my $t (@{ $opt{file} }) {

        next if ! exists $t->{latest};

        if ($t->{latest} ne $t->{lastfile} and $t->{latest} !~ /SERVICE=/) {
            $changes++;
            $t->{lastfile} = delete $t->{latest};
            next;
        }
        if (exists $opt{newoffset}{$t->{latest}}) {
            my $newoffset = $opt{newoffset}{$t->{latest}};
            if ($t->{offset} != $newoffset) {
                $changes++;
                $t->{offset} = $newoffset;
            }
        }
        if (exists $opt{newmodtime}{$t->{latest}}) {
            my $newmodtime = $opt{newmodtime}{$t->{latest}};
            $t->{modtime} ||= 0;
            if ($t->{modtime} ne $newmodtime) {
                $changes++;
                $t->{modtime} = $newmodtime;
            }
        }
        if (exists $opt{newrotatefilemodtime}{$t->{latest}}) {
            my $newrotatemodtime = $opt{newrotatefilemodtime}{$t->{latest}};
            ## Special case where the rotated file has disappeared: remove from config
            if ($newrotatemodtime eq 'rotated_file_gone') {
                delete $t->{rotatefilemodtime};
                $changes++;
            }
            else {
                $t->{rotatefilemodtime} ||= 0;
                if ($t->{rotatefilemodtime} ne $newrotatemodtime) {
                    $changes++;
                    $t->{rotatefilemodtime} = $newrotatemodtime;
                }
            }
        }
    }
    ## No rewriting if in regular dryrun mode, but reset always trumps dryrun
    ## Otherwise, do nothing if there have been no changes
    return if (!$changes or 1==$arg{dryrun}) and !$arg{reset};

    $arg{verbose} and warn "  Saving new config file\n";
    open my $sfh, '>', $statefile or die qq{Could not write "$statefile": $!\n};
    my $oldselect = select $sfh;
    my $now = localtime;
    print qq{## State file for the tail_n_mail program
## Please see this file for complete information: $configfile
## This file is automatically updated
## Last updated: $now

};

    for my $f (sort { $a->{suffix} <=> $b->{suffix} }
               @{ $opt{file} }) {

        printf "\nFILE%d: %s\n", $f->{suffix}, $f->{original};

        ## Got any lastfile or offset for these?
        if ($f->{lastfile} and $f->{original} !~ /SERVICE=/) {
            printf "LASTFILE%d: %s\n", $f->{suffix}, $f->{lastfile};
        }
        ## Got a rotated file mod time?
        if (exists $f->{rotatefilemodtime}) {
            printf "ROTATEFILEMODTIME%d: %s\n", $f->{suffix}, $f->{rotatefilemodtime};
        }
        ## The offset may be new, or we may be the same as last time
        if (defined $f->{latest} and exists $opt{newoffset}{$f->{latest}}) {
            printf "OFFSET%d: %d\n", $f->{suffix}, $opt{newoffset}{$f->{latest}};
        }
        elsif (defined $f->{offset}) {
            printf "OFFSET%d: %d\n", $f->{suffix}, $f->{offset};
        }
        ## May have a modtime
        if ($f->{modtime}) {
            printf "MODTIME%d: %s\n", $f->{suffix}, $f->{modtime};
        }
    }
    print "\n";

    select $oldselect;
    close $sfh or die qq{Could not close "$statefile": $!\n};

    return;

} ## end of final_cleanup


sub add_comments {

    my $item = shift;
    return if ! exists $itemcomment{$item};
    for my $comline (@{$itemcomment{$item}}) {
        print $comline;
    }

    return;

} ## end of add_comments


sub pretty_number {

    ## Format a raw number in a more readable style

    my $number = shift;

    die caller() if ! defined $number;

    return $number if $number !~ /^[0-9]+$/ or $number < 1000;

    ## If this is our first time here, find the correct separator
    if (! defined $arg{tsep}) {
        my $lconv = localeconv();
        $arg{tsep} = $lconv->{thousands_sep} || ',';
    }

    ## No formatting at all
    return $number if '' eq $arg{tsep} or ! $arg{tsep};

    (my $reverse = reverse $number) =~ s/(...)(?=[0-9])/$1$arg{tsep}/g;
    $number = reverse $reverse;
    return $number;

} ## end of pretty_number


sub pretty_size {

    ## Transform number of bytes to a SI display similar to Postgres' format

    my $bytes = shift;
    my $rounded = shift || 0;

    return "$bytes bytes" if $bytes < 10_240;

    my @unit = qw/kB MB GB TB PB EB YB ZB/;

    for my $p (1..@unit) {
        if ($bytes <= 1024**$p) {
            $bytes /= (1024**($p-1));
            return $rounded ?
                sprintf ('%d %s', $bytes, $unit[$p-2]) :
                    sprintf ('%.2f %s', $bytes, $unit[$p-2]);
        }
    }

    return $bytes;

} ## end of pretty_size



__DATA__

## Example config file:

## Config file for the tail_n_mail program
## This file is automatically updated
EMAIL: someone@example.com
MAILSUBJECT: Acme HOST Postgres errors UNIQUE : NUMBER

FILE: /var/log/postgres/postgres-%Y-%m-%d.log
INCLUDE: ERROR:  
INCLUDE: FATAL:  
INCLUDE: PANIC:  


