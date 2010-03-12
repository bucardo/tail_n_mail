#!/usr/bin/env perl

## Tail one or more files, mail the new stuff to one or more emails
## Developed at End Point Corporation by:
## Greg Sabino Mullane <greg@endpoint.com>
## Selena Deckelmann <selena@endpoint.com>
## BSD licensed
## For full documentation, please see: http://bucardo.org/wiki/Tail_n_mail

##
## Quick usage:
## Run: tail tail_n_mail.pl > tail_n_mail.config
## Edit tail_n_mail.config in your favorite editor
## Run: perl tail_n_mail.pl tail_n_mail.config
## Once working, put the above into a cron job

use strict;
use warnings;
use Data::Dumper   qw( Dumper     );
use Getopt::Long   qw( GetOptions );
use File::Temp     qw( tempfile   );
use File::Basename qw( dirname    );
use 5.008003;

our $VERSION = '1.8.0';

## Location of the sendmail program. Expects to be able to use a -f argument.
my $MAILCOM = '/usr/sbin/sendmail';

## We never go back more than this number of bytes. Can be overriden in the config file and command line.
my $MAXSIZE = 80_000_000;

## Default message subject if not set elsewhere. Keywords replaced: FILE HOST
my $DEFAULT_SUBJECT= 'Results for FILE on host: HOST';

## We can define custom types, e.g. "duration" that get printed differently
my $custom_type = 'normal';

## Set defaults for all the options, then read them in from command line
my ($verbose,$quiet,$debug,$dryrun,$reset,$limit,$rewind,$version) = (0,0,0,0,0,0,0,0);
my ($custom_offset,$custom_duration,$custom_file,$nomail,$flatten) = (-1,-1,'',0,1);
my ($timewarp,$pgmode,$find_line_number,$pgformat,$maxsize) = (0,1,1,1,$MAXSIZE);

my $result = GetOptions
 (
   'verbose'    => \$verbose,
   'quiet'      => \$quiet,
   'debug'      => \$debug,
   'dryrun'     => \$dryrun,
   'nomail'     => \$nomail,
   'reset'      => \$reset,
   'limit=i'    => \$limit,
   'rewind=i'   => \$rewind,
   'version'    => \$version,
   'offset=i'   => \$custom_offset,
   'duration=i' => \$custom_duration,
   'file=s'     => \$custom_file,
   'flatten!'   => \$flatten,
   'timewarp=i' => \$timewarp,
   'pgmode=i'   => \$pgmode,
   'pgformat=i' => \$pgformat,
   'maxsize=i'  => \$maxsize,
   'type=s'     => \$custom_type,
  );
++$verbose if $debug;

if ($version) {
	print "$0 version $VERSION\n";
	exit 0;
}

## First option is always the config file, which must exist.
my $configfile = shift or die qq{Usage: $0 configfile\n};

## If the file has the name 'duration' in it, switch to that type as the default
if ($configfile =~ /duration/i) {
	$custom_type = 'duration';
}

## Save away our hostname
my $hostname = qx{hostname};
chomp $hostname;

## Regexen for Postgres PIDs:
my %pgpidres = (
   1 => qr{.+?\[(\d+)\]: \[(\d+)\-(\d+)\]},
   2 => qr{.+?\d\d:\d\d:\d\d \w\w\w (\d+)},
   3 => qr{.+?\d\d:\d\d:\d\d (\w\w\w)}, ## Fake a PID
   4 => qr{.+?\[(\d+)\]},
);

my $pgpidre = $pgpidres{$pgformat};

## Regexes to extract the date part from a line:
my %pgpiddateres = (
	1 => qr{(.+?\[\d+\]): \[\d+\-\d+\]},
	2 => qr{(.+?\d\d:\d\d:\d\d \w\w\w)},
	3 => qr{(.+?\d\d:\d\d:\d\d \w\w\w)},
	4 => qr{(.+? \w\w\w)},
);
my $pgpiddatere = $pgpiddateres{$pgformat};

## The possible types of levels in Postgres logs
my %level = map { $_ => 1 } qw{PANIC FATAL ERROR WARNING NOTICE LOG INFO};
for (1..5) { $level{"DEBUG$_"} = 1; }
my $levels = join '|' => keys %level;
my $levelre = qr{(?:$levels)};

## We use a CURRENT key for future expansion
my $curr = 'CURRENT';

## Read in and parse the config file
my (%opt, %itemcomment);
parse_config_file();

## Keep track of changes to know if we need to rewrite the config file or not
my $changes = 0;

## Global regex: may change per file
my ($exclude, $include);

## Note if we bumped into maxsize when trying to read a file
my (%toolarge);

# Actual matching strings are stored here
my %find;

## Keep track of which entries are similar to the ones we've seen before for possible flattening
my %similar;

## Map filenames to "A", "B", etc. for clean output of multiple matches
my %fab;

## Are we viewing the older version of the file because it was rotated?
my $rotatedfile = 0;

## Did we handle more than one file this round?
my $multifile = 0;

## Total matches across all files
$opt{grand_total} = 0;

## Parse each file returned by pick_log_file until we start looping
my $last_logfile = '';
my @files_parsed;
{
	my $logfile = pick_log_file();
	last if $last_logfile eq $logfile;
	$debug and warn "Parsing file ($logfile)\n";
	my $count = parse_file($logfile);
	push @files_parsed => [$logfile, $count];
	$last_logfile = $logfile;
	redo;
}

## We're done parsing the message, send an email if needed
process_report() if $opt{grand_total};
final_cleanup();

exit 0;


sub pick_log_file {

	## Figure out which files we need to parse

	## Basic flow:
	## Start with "last" (and apply offset to it)
	## Then walk forward until we hit the most recent one

	## If a custom file, we always just return the main filename
	## We also remove the lastfile, as it's not important anymore
	if ($custom_file) {
		delete $opt{$curr}{lastfile};
		return $opt{$curr}{filename};
	}

	## No lastfile makes it easy
	exists $opt{$curr}{lastfile} or return $opt{$curr}{filename};

	my $last = $opt{$curr}{lastfile};

	## If we haven't processed the lastfile, do that one first
	exists $find{$last} or return $last;

	## If the last is the same as the current, return
	$last eq $opt{$curr}{filename} and return $last;

	## We've processed the last file, are there any files in between the two?
	## For now, we only handle POSIX-based time travel
	my $orig = $opt{$curr}{original_filename};
	if ($orig =~ /%/) {

		## Already have the list? Pop off items until we are done
		if (exists $opt{$curr}{middle_filenames}) {
			my $newfile = pop @{$opt{$curr}{middle_filenames}};
			## When we run out, return the current file
			return $newfile || $opt{$curr}{filename};
		}

		## We're going to walk backwards, 30 minutes at a time, and gather up
		## all files between "now" and the "last"
		my $timerewind = 60*30; ## 30 minutes
		my $maxloops = 24*2 * 7; ## ax of 1 week
		my $bail = 0;
		my %seenfile;
	  BACKINTIME: {

			my @ltime = localtime(time - $timerewind);
			my $newfile = POSIX::strftime($orig, @ltime); ## no critic (ProhibitCallsToUnexportedSubs)
			last if $newfile eq $last;
			if (! exists $seenfile{$newfile}) {
				$seenfile{$newfile} = 1;
				push @{$opt{$curr}{middle_filenames}} => $newfile;
			}

			$timerewind += 60*30;
			++$bail > $maxloops and die "Too many loops ($bail): bailing\n";
			redo;
		}

		return (keys %seenfile) ? (pop @{$opt{$curr}{middle_filenames}}) : $opt{$curr}{filename};
	}

	## Just return the current file
	return $opt{$curr}{filename};

} ## end of pick_log_file


sub parse_config_file {

	## Read in a configuration file and populate the global %opt

	## Are we in the standard onn-user comments at the top of the file?
	my $in_standard_comments = 1;

	## Temporarily store user comments until we know where to put them
	my (@comment);

	open my $c, '<', $configfile or die qq{Could not open "$configfile": $!\n};
	$debug and warn qq{Opened config file "$configfile"\n};
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

		## A non-comment after one or comments allows us to map them to each other
		if (@comment and m{^(\w+):}) {
			chomp;
			for my $c (@comment) {
				## We store as both the keyword and the entire line
				push @{$itemcomment{$1}} => $c;
				push @{$itemcomment{$_}} => $c;
			}
			## Empty out our user comment queue
			undef @comment;
		}

		## What file are we checking on?
		if (/^FILE:\s*(.+?)\s*$/) {
			my $filename = $opt{$curr}{original_filename} = $1;

			if ($filename !~ /\w/) {
				die "No FILE found in the config file! (tried: $filename)\n";
			}

			## Transform the file name if it contains escapes
			if ($filename =~ /%/) {
				eval { ## no critic (RequireCheckingReturnValueOfEval)
					require POSIX;
				};

				$@ and die qq{Cannot use strftime formatting without the Perl POSIX module!\n};

				## Allow moving back in time with the timewarp argument (defaults to 0)
				my @ltime = localtime(time + $timewarp);
				$filename = POSIX::strftime($filename, @ltime); ## no critic (ProhibitCallsToUnexportedSubs)
			}

			## Transform the file name if they want the latest in a directory
			## Note that this can be combined with the escapes above!
			if ($filename =~ /LATEST/) {
				$filename =~ s/LATEST/*/;
				my $latest = qx{ls -rt1 $filename | tail -1};
				chomp $latest;
				$filename = $latest;
			}

			## If a custom file was specified, use that instead
			if ($custom_file) {
				## If it contains a path, use it directly
				if ($custom_file =~ m{/}) {
					$filename = $custom_file;
				}
				## Otherwise, replace the current file name but keep the directory
				else {
					my $dir = dirname($filename);
					$filename = "$dir/$custom_file";
				}
			}

			## Set some default values
			$opt{$curr}{filename} = $filename;
			$opt{$curr}{exclude} ||= [];
			$opt{$curr}{include} ||= [];
			$opt{$curr}{email}   ||= [];

		} ## end of FILE:

		## The last filename we used
		elsif (/^LASTFILE:\s*(.+?)\s*$/) {
			$opt{$curr}{lastfile} = $1;
		}
		## Who to send emails to for this file
		elsif (/^EMAIL:\s*(.+?)\s*$/) {
			push @{$opt{$curr}{email}}, $1;
		}
		## Who to send emails from
		elsif (/^FROM:\s*(.+?)\s*$/) {
			$opt{$curr}{from} = $1;
		}
		## The pg format to use
		elsif (/^PGFORMAT:\s*(\d)\s*$/) {
			## Change the global $pgformat as well
			$opt{$curr}{pgformat} = $pgformat = $1;
			## Assign new regex, bail if they don't exist
			$pgpidre = $pgpidres{$pgformat} or die "Invalid PGFORMAT line: $pgformat\n";
			$pgpiddatere = $pgpiddateres{$pgformat} or die "Invalid PGFORMAT line: $pgformat\n";
		}
		## What type of report this is
		elsif (/^TYPE:\s*(.+?)\s*$/) {
			$custom_type = $1;
		}
		## Exclude durations below this number
		elsif (/^DURATION:\s*(\d+)/) {
			## Command line still wins
			if ($custom_duration < 0) {
				$custom_duration = $opt{$curr}{duration} = $1;
			}
		}
		## Force line number lookup on or off
		elsif (/^FIND_LINE_NUMBER:\s*(\d+)/) {
			$find_line_number = $opt{$curr}{find_line_number} = $1;
		}
		## Which lines to exclude from the report
		elsif (/^EXCLUDE:\s*(.+?)\s*$/) {
			push @{$opt{$curr}{exclude}}, $1;
		}
		## Which lines to include in the report
		elsif (/^INCLUDE:\s*(.+)/) {
			push @{$opt{$curr}{include}}, $1;
		}
		## The current offset into the file
		elsif (/^OFFSET:\s*(\d+)/) {
			$opt{$curr}{offset} = $1;
		}
		## The custom maxsize for this file
		elsif (/^MAXSIZE:\s*(\d+)/) {
			$opt{$curr}{maxsize} = $1;
		}
		## The subject line for this file
		elsif (/^MAILSUBJECT:\s*(.+)/) { ## Trailing whitespace is significant here
			$opt{$curr}{mailsubject} = $1;
			$opt{$curr}{customsubject} = 1;
		}
	}
	close $c or die qq{Could not close "$configfile": $!\n};

	if ($debug) {
		local $Data::Dumper::Varname = 'opt';
		warn Dumper \%opt;
	}

	## Set the maximum bytes to go back and scan
	$maxsize = exists $opt{$curr}{maxsize} ? $opt{$curr}{maxsize} : $maxsize;

	return;

} ## end of parse_config_file


sub parse_file {

	## Parse the passed in file
	## Returns the number of matches

	my $filename = shift;

	## Touch the hash so we know we've been here
	$find{$filename} = {};

	## Make sure the file exists and is readable
	if (! -e $filename) {
		$quiet or warn qq{WARNING! Skipping non-existent file "$filename"\n};
		return 0;
	}
	if (! -f $filename) {
		$quiet or warn qq{WARNING! Skipping non-file "$filename"\n};
		return 0;
	}

	## Figure out where in the file we want to start scanning from
	my $size = -s $filename;
	my $offset = 0;

	## Is the offset significant?
	## Usually only is if the stored offset matches the current file

	if (!exists $opt{$curr}{lastfile} or ($opt{$curr}{lastfile} eq $filename)) {
		## Allow the offset to equal the size via --reset
		if ($reset) {
			$offset = $size;
			$verbose and warn "  Resetting offset to $offset\n";
		}
		## Allow the offset to be changed on the command line
		elsif ($custom_offset != -1) {
			if ($custom_offset >= 0) {
				$offset = $custom_offset;
			}
			elsif ($custom_offset < -1) {
				$offset = $size + $custom_offset;
				$offset = 0 if $offset < 0;
			}
		}
		else {
			$offset = $opt{$curr}{offset};
		}
	}

	$verbose and warn "  File: $filename Offset: $offset Size: $size Maxsize: $maxsize\n";

	## The file may have shrunk due to a logrotate
	if ($offset > $size) {
		$verbose and warn "  File has shrunk - resetting offset to 0\n";
		$offset = 0;
	}

	## If the offset is equal to the size, we're done!
	return 0 if $offset >= $size;

	## Store the original offset
	my $original_offset = $offset;


	## This can happen quite a bit on busy files!
	if ($maxsize and ($size - $offset > $maxsize) and $custom_offset < 0) {
		$quiet or warn "  SIZE TOO BIG (size=$size, offset=$offset): resetting to last $maxsize bytes\n";
		$toolarge{$filename} = "File too large: only read last $maxsize bytes (size=$size, offset=$offset)";
		$offset = $size - $maxsize;
	}

	open my $fh, '<', $filename or die qq{Could not open "$filename": $!\n};

	## Seek the right spot as needed
	if ($offset and $offset < $size) {

		## Because we go back by 10 characters below, always offset at least 10
		$offset = 10 if $offset < 10;

		## We go back 10 characters to get us before the newlines we (probably) ended with
		seek $fh, $offset-10, 0;

		## If a manual rewind request has been given, process it (inverse)
		if ($rewind) {
			seek $fh, -$rewind, 1;
		}
	}

	## Optionally figure out what approximate line we are on
	my $newlines = 0;
	if ($find_line_number) {
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
				my $res = sysread $fh, $foobar, $chunksize;
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
	($exclude,$include) = generate_regexes($filename);

	## Discard the previous line if needed (we rewound by 10 characters above)
	$original_offset and <$fh>;

	## Keep track of matches for this file
	my $count = 0;

	## Postgres-specific multi-line grabbing stuff:
	my ($pgpid, $pgnum, $pgsub, %pidline, %current_pid_num, $lastpid);

	my $lastline = '';
	while (<$fh>) {
		## Easiest to just remove the newline here and now
		chomp;
		if ($pgmode) {
			if ($_ =~ $pgpidre) {
				($pgpid, $pgsub, $pgnum) = ($1,$2||1,$3||1);
				$lastpid = $pgpid;
				## We've found something that looks like: postgres[12345] [1-1]
				## Have we seen this PID before?
				if (exists $pidline{$pgpid}) {
					## If this is a statement or detail or hint or context, append to the previous entry
					## Do the same for a LOG: statement combo (e.g. following a duration)
					if (/ (?:STATEMENT|DETAIL|HINT|CONTEXT):  /o
							or (/ LOG:  statement: /o
							and
							$pidline{$pgpid}{string}{$current_pid_num{$pgpid}}
							or (/ LOG:  statement: /o and $lastline !~ /  statement: |FATAL|PANIC/))) {
						$pgnum = $current_pid_num{$pgpid} + 1;
					}
					## Is this a new entry for this PID? If so, process the old.
					if ($pgnum <= 1) {
						$count += process_line($pidline{$pgpid}, 0, $filename);
						## Delete it so it gets recreated afresh below
						delete $pidline{$pgpid};
					}
					else {
						## Trim the string up - no need to see the header info each time
						s/^.+?$pgpidre\s*//;
						## No need to see the user more than once for supplemental information
						s/.+ (STATEMENT|DETAIL|HINT|CONTEXT):/$1:/;
					}
				}
				$pidline{$pgpid}{string}{$pgnum} = $_;
				$current_pid_num{$pgpid} = $pgnum;

				## Store the line number if we don't have one yet for this PID
				$pidline{$pgpid}{line} ||= ($. + $newlines);

				$lastline = $_;
			}
			elsif ($lastpid) {
				## Append this to the previous entry
				$pgnum = $current_pid_num{$lastpid} + 1;
				s{^\t}{ };
				$pidline{$lastpid}{string}{$pgnum} = $_;
				$current_pid_num{$lastpid} = $pgnum;
			}

			## No need to do anything more right now
			next;
		}

		## Just a bare entry, so process it right away
		$count += process_line($_, $. + $newlines, $filename);

	} ## end of each line in the file

	## Get the new offset and store it
	seek $fh, 0, 1;
	$opt{$curr}{newoffset} = tell $fh;

	close $fh or die qq{Could not close "$filename": $!\n};

	## Now add in any pids that have not been processed yet
	for my $pid (keys %pidline) {
		$count += process_line($pidline{$pid}, 0, $filename);
	}

	if (!$count) {
		$verbose and warn "  No new lines found in file $filename\n";
	}
	else {
		$verbose and warn "  Lines found in $filename: $count\n";
	}

	$opt{grand_total} += $count;

	return $count;

} ## end of parse_file


sub generate_regexes {

	## Given a filename, generate exclusion and inclusion regexes for it

	## Currently, all files get the same regex, so we cache it
	if (exists $opt{globalexcluderegex}) {
		return $opt{globalexcluderegex}, $opt{globalincluderegex};
	}

	## Build an exclusion regex
	my $exclude = '';
	for my $ex (@{$opt{$curr}{exclude}}) {
		$debug and warn "  Adding exclusion: $ex\n";
		my $regex = qr{$ex};
		$exclude .= "$regex|";
	}
	$exclude =~ s/\|$//;
	$verbose and $exclude and warn "  Exclusion: $exclude\n";

	## Build an inclusion regex
	my $include = '';
	for my $in (@{$opt{$curr}{include}}) {
		$debug and warn "  Adding inclusion: $in\n";
		my $regex = qr{$in};
		$include .= "$regex|";
	}
	$include =~ s/\|$//;
	$verbose and $include and warn "  Inclusion: $include\n";

	$opt{globalexcluderegex} = $exclude;
	$opt{globalincluderegex} = $include;

	return $exclude, $include;

} ## end of generate_regexes


sub process_line {

	## We've got a complete statement, so do something with it!
	## If it matches, we'll either put into %find directly, or store in %similar

	my $arg = shift;

	## What line was this string first spotted at?
	my $line = shift || 0;

	## What file did this come from?
	my $filename = shift || die "Need a filename!\n";

	## The final string
	my $string = '';

	if (ref $arg eq 'HASH') {
		for my $l (sort {$a<=>$b} keys %{$arg->{string}}) {
			## Some Postgres/syslog combos produce ugly output
			$arg->{string}{$l} =~ s/^(?:\s*#011\s*)+//;
			$string .= ' '.$arg->{string}{$l};
		}
		$line = $arg->{line};
	}
	else {
		$string = $arg;
	}

	## Bail if it does not match the inclusion regex
	return 0 if $include and $string !~ $include;

	## Bail if it matches the exclusion regex
	return 0 if $exclude and $string =~ $exclude;

	## If in duration mode, and we have a minimum cutoff, discard faster ones
	if ($custom_type eq 'duration' and $custom_duration >= 0) {
		return 0 if ($string =~ / duration: (\d+)/ and $1 < $custom_duration);
	}

	$debug and warn "MATCH at line $line of $filename\n";

	## Compress all whitespace
	$string =~ s/\s+/ /g;

	## Strip leading whitespace
	$string =~ s/^\s+//;

	## If not in Postgres mode, we avoid all the mangling below
	if (!$pgmode) {
		$find{$filename}{$line} =
				{
				 string   => $string,
				 nonflat  => $string,
				 line     => $line,
				 filename => $filename,
				 count    => 1,
				 };
		return 1;
	}

	## Save the pre-flattened version
	my $nonflat = $string;

	## Make some adjustments to attempt to compress similar entries
	if ($flatten) {

		## Flatten simple case of 'foo,bar' right away
		$string =~ s/'[\w\d ]+\s*,\s*[\w\d ]+'/?/go;

		$string =~ s{(VALUES|REPLACE)\s*\((.+)\)}{
			my $final = '';
			my @final = split /\s*,\s*/ => $2;
			for my $section (@final) {
				next if $section =~ s/E?'.*'/?/go;
				next if $section =~ s/^\d+$/?/o;
				next if $section =~ s/true|false/?/go;
			}
			"$1 (" . (join ',' => @final) . ')'
			}geix;
		$string =~ s{(\bWHERE\s+\w+\s*=\s*)\d+}{$1?}gio;
		$string =~ s{(\bWHERE\s+\w+\s+IN\s*\().+?\)}{$1?)}gio;
		$string =~ s{(UPDATE\s+\w+\s+SET\s+\w+\s*=\s*)'[^']*'}{$1'?'}go;
		$string =~ s/(ERROR:  invalid byte sequence for encoding "UTF8": 0x)[a-f0-9]+/$1????/o;
		$string =~ s{(\(simple_geom,)'.+?'}{$1'???'}gio;
	}

	## Try to separate into header and body, then check for similar entries
	if ($string =~ /(.+?)($levelre:.+)$/o) {
		my ($head,$body) = ($1,$2);

		## Seen this body before?
		my $seenit = 0;

		if (exists $similar{$body}) {
			$seenit = 1;
			## This becomes the new latest one
			$similar{$body}{latest} =
				{
				 string   => $string,
				 nonflat  => $nonflat,
				 line     => $line,
				 filename => $filename,
				 };
			## Increment the count
			$similar{$body}{count}++;
		}

		if (!$seenit) {
			## Store as the earliest and latest version we've seen
			$similar{$body}{earliest} = $similar{$body}{latest} =
				{
				 string   => $string,
				 nonflat  => $nonflat,
				 line     => $line,
				 filename => $filename,
				 };
			## Start counting these items
			$similar{$body}{count} = 1;
			## Store this away for eventual output
			$find{$filename}{$line} = $similar{$body};
		}
	}
	else {
		$find{$filename}{$line} = $string;
	}

	return 1;

} ## end of process_line


sub process_report {

	## Create the mail message
	my ($fh, $tempfile) = tempfile('tnmXXXXXXXX', SUFFIX => '.tnm');

	if ($dryrun) {
		close $fh or warn 'Could not close filehandle';
		$fh = \*STDOUT;
	}

	if (! @files_parsed) {
		$quiet or warn qq{No files were read in, exiting\n};
		exit 1;
	}

	## Total matches across all files
	my $total_matches = 0;
	## How many files actually had things?
	my $matchfiles = 0;
	## Which was the latest to contain something?
	my $last_file_parsed;
	for my $file (@files_parsed) {
		next if ! $file->[1];
		$total_matches += $file->[1];
		$matchfiles++;
		$last_file_parsed = $file->[0];
	}
	## If not files matched, output the last one processed
	$last_file_parsed = $files_parsed[-1]->[0] if ! defined $last_file_parsed;

	## Subject with replaced keywords:
	my $subject = $opt{$curr}{mailsubject} || $DEFAULT_SUBJECT;
	$subject =~ s/FILE/$last_file_parsed/g;
	$subject =~ s/HOST/$hostname/g;
	print {$fh} "Subject: $subject\n";

	## Discourage vacation programs from replying
	print {$fh} "Auto-Submitted: auto-generated\n";
	print {$fh} "Precedence: bulk\n";

	## Some minor help with debugging
	print {$fh} "X-TNM-VERSION: $VERSION\n";

	## Fill out the "To:" fields
	for my $email (@{$opt{$curr}{email}}) {
		print {$fh} "To: $email\n";
	}
	if (! @{$opt{$curr}{email}}) {
		die "Cannot send email without knowing who to send to!\n";
	}

	## Custom From:
	my $from_addr = $opt{$curr}{from} || '';
	if ($from_addr ne '') {
		print {$fh} "From: $from_addr\n";
		$MAILCOM .= " -f $from_addr";
	}
	## End header section
	print {$fh} "\n";

	my $now = scalar localtime;
	print {$fh} "Date: $now\n";
	print {$fh} "Host: $hostname\n";
	if ($timewarp) {
		print {$fh} "Timewarp: $timewarp\n";
	}
	if ($custom_duration >= 0) {
		print {$fh} "Minimum duration: $custom_duration\n";
	}

	## If we parsed more than one file, label them now
	if ($matchfiles > 1) {
		my $letter = 0;
		print {$fh} "Total matches: $opt{grand_total}\n";
		for my $file (@files_parsed) {
			next if ! $file->[1];
			my $name = chr(65+$letter);
			if ($letter >= 26) {
				$name = sprintf '%s%s',
					chr(64+($letter/26)), chr(65+($letter%26));
			}
			$letter++;

			printf {$fh} "Matches from [%s]%s%s: %d\n",
				$name,
				(length $name > 1) ? ' ' : '  ',
				$file->[0],
				$file->[1];
			$fab{$file->[0]} = $name;
		}
	}
	else {
		print {$fh} "Matches from $last_file_parsed: $total_matches\n";
	}

	for my $file (@files_parsed) {
		if (exists $toolarge{$file->[0]}) {
			print {$fh} "$toolarge{$file->[0]}\n";
		}
	}

	## The meat of the message
	lines_of_interest($fh, $matchfiles);

	print {$fh} "\n";
	close $fh or die qq{Could not close "$tempfile": $!\n};

	my $emails = join ' ' => @{$opt{$curr}{email}};
	$verbose and warn "  Sending mail to: $emails\n";
	my $COM = qq{$MAILCOM $emails < $tempfile};
	if ($dryrun or $nomail) {
		$quiet or warn "  DRYRUN: $COM\n";
	}
	else {
		system $COM;
	}
	unlink $tempfile;

	return;

} ## end of process_report


sub lines_of_interest {

	## Given a file handle, print all our current lines to it

	my ($lfh,$matchfiles) = @_;

	my $oldselect = select $lfh;

	our ($current_filename, %sorthelp);

	sub sortsub {
		if ($custom_type eq 'duration') {
			if (! exists $sorthelp{$current_filename}{$a}) {
				$sorthelp{$current_filename}{$a} =
					$find{$current_filename}{$a}{earliest}{string} =~ /duration: (\d+\.\d+)/ ? $1 : 0;
			}
			if (! exists $sorthelp{$current_filename}{$b}) {
				$sorthelp{$current_filename}{$b} =
					$find{$current_filename}{$b}{earliest}{string} =~ /duration: (\d+\.\d+)/ ? $1 : 0;
			}
			return ($sorthelp{$current_filename}{$b} <=> $sorthelp{$current_filename}{$a} or $a <=> $b);
		}

		return $a <=> $b;
	}

	## Display all lines in some sort of order
	## Default is per file parsed, then per line

	my $count = 1;
  FIHL: for my $file (@files_parsed) {
		next if ! $file->[1];
		$current_filename = $file->[0];
	  LIHN: for my $findline (sort sortsub keys %{$find{$current_filename}}) {
			print "\n[$count]";
			$count++;
			my $f = $find{$current_filename}{$findline};

			my $filename = exists $f->{earliest} ? $f->{earliest}{filename} : $f->{filename};

			## If only a single entry, simpler output
			if ($f->{count} == 1) {
				if ($matchfiles > 1) {
					printf " From file %s%s\n",
						$fab{$filename},
						$find_line_number ?  " (line $findline)" : '';
				}
				else {
					($find_line_number) and print " (from line $findline)\n";
				}
				printf "%s\n", exists $f->{earliest} ? $f->{earliest}{string} : $f->{string};
				next LIHN;
			}

			## More than one entry means we have an earliest and latest to look at
			my $earliest = $f->{earliest};
			my $latest = $f->{latest};

			## Does it span multiple files?
			my $samefile = $earliest->{filename} eq $latest->{filename} ? 1 : 0;
			if ($samefile) {
				if ($matchfiles > 1) {
					print " From file $fab{$filename}";
					if ($find_line_number) {
						print " (between lines $findline and $latest->{line}, occurs $f->{count} times)";
					}
					print "\n";
				}
				else {
					if ($find_line_number) {
						print " (between lines $findline and $latest->{line}, occurs $f->{count} times)";
					}
					print "\n";
				}
			}
			else {
				my ($A,$B) = ($fab{$earliest->{filename}}, $fab{$latest->{filename}});
				print " From files $A and $B";
				if ($find_line_number) {
					print " (between lines $findline of $A and $latest->{line} of $B, occurs $f->{count} times)";
				}
				print "\n";
			}

			## If we can, show just the interesting part
			my $estring = $earliest->{string};
			my $lstring = $latest->{string};
			if ($pgmode == 1 and $estring =~ s/$pgpiddatere(.+)/$2/) {
				my $headstart = $1;
				$lstring =~ /$pgpiddatere/ or die "Latest did not match?!\n";
				my $headend = $1; ## no critic (ProhibitCaptureWithoutTest)
				printf "First: %s%s\nLast:  %s%s\n",
					$samefile ? '' : "[$fab{$earliest->{filename}}] ",
					$headstart,
					$samefile ? '' : "[$fab{$latest->{filename}}] ",
					$headend;
				$estring =~ s/^\s+//;
				print "$estring\n";
			}
			else {
				print " Earliest and latest:\n";
				print "$estring\n$lstring\n";
			}

		} ## end each line
	} ## end each file

	select $oldselect;

	return;

} ## end of lines_of_interest


sub final_cleanup {

	$debug and warn "  Performing final cleanup\n";

	## If offset has changed, save it
	my $newoffset = 0;
	if (exists $opt{$curr}{newoffset}) {
		$changes++;
		$newoffset = $opt{$curr}{newoffset};
		$verbose and warn "  Setting offset to $newoffset\n";
	}

	if ($changes and !$dryrun) {
		$verbose and warn "Saving new config file (changes=$changes)\n";
		open my $fh, '>', $configfile or die qq{Could not write "$configfile": $!\n};
		my $oldselect = select $fh;
		my $now = localtime;
		print qq{
## Config file for the tail_n_mail.pl program
## This file is automatically updated
## Last updated: $now
};

		for my $item (qw/ email from pgformat type duration find_line_number /) {
			next if ! exists $opt{$curr}{$item};
			next if $item eq 'duration' and $custom_duration < 0;
			add_comments(uc $item);
			if (ref $opt{$curr}{$item} eq 'ARRAY') {
				for my $itemz (@{$opt{$curr}{$item}}) {
					printf "%s: %s\n", uc $item, $itemz;
				}
			}
			else {
				printf "%s: %s\n", uc $item, $opt{$curr}{$item};
			}
		}
		printf "MAXSIZE: %d\n",
			exists $opt{$curr}{maxsize} ? $opt{$curr}{maxsize} : $maxsize;
		if ($opt{$curr}{customsubject}) {
			add_comments('MAILSUBJECT');
			print "MAILSUBJECT: $opt{$curr}{mailsubject}\n";
		}

		print "\n";
		add_comments('FILE');
		print "FILE: $opt{$curr}{original_filename}\n";
		print "LASTFILE: $opt{$curr}{filename}\n";
		print "OFFSET: $newoffset\n";
		for my $include (@{$opt{$curr}{include}}) {
			add_comments("INCLUDE: $include");
			print "INCLUDE: $include\n";
		}
		for my $exclude (@{$opt{$curr}{exclude}}) {
			add_comments("EXCLUDE: $exclude");
			print "EXCLUDE: $exclude\n";
		}
		print "\n";

		select $oldselect;
		close $fh or die qq{Could not close "$configfile": $!\n};
	}

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


__DATA__

## Example config file:

## Config file for the tail_n_mail.pl program
## This file is automatically updated
EMAIL: someone@example.com

FILE: /var/log/postgresql-%Y-%m-%d.log
INCLUDE: ERROR:  
INCLUDE: FATAL:  
INCLUDE: PANIC:  
MAILSUBJECT: Acme HOST Postgres errors (FILE)

