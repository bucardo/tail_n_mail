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
use Data::Dumper   qw( Dumper           );
use File::Temp     qw( tempfile         );
use Getopt::Long   qw( GetOptions       );
use File::Basename qw( basename dirname );
use 5.008003;

our $VERSION = '1.7.1';

my $me = basename($0);
my $hostname = qx{hostname};
chomp $hostname;

## Number the entries?
my $USE_NUMBERING = 1;

## Get the line number from the log file, even when seeking
my $find_line_number = 1;

## Regexen for Postgres PIDs:
my %pgpidres = (
   1 => qr{.+?\[(\d+)\]: \[(\d+)\-(\d+)\]},
   2 => qr{.+?\d\d:\d\d:\d\d \w\w\w (\d+)},
   3 => qr{.+?\d\d:\d\d:\d\d (\w\w\w)}, ## Fake a PID
   4 => qr{.+?\[(\d+)\]},
);
my $pgformat = 1;
my $pgpidre = $pgpidres{$pgformat};

## Regexes to extract the date part from a line:
my %pgpiddateres = (
    1 => qr{(.+?\[\d+\]): \[\d+\-\d+\]},
    2 => qr{(.+?\d\d:\d\d:\d\d \w\w\w)},
    3 => qr{(.+?\d\d:\d\d:\d\d \w\w\w)},
    4 => qr{(.+? \w\w\w)},
);
my $pgpiddatere = $pgpiddateres{$pgformat};

## The mail program
my $MAILCOM = '/usr/sbin/sendmail';

## We never go back more than this number of bytes. Can be overriden in the config file.
my $MAXSIZE = 80_000_000;

## Default message subject if not set elsewhere. Keywords replaced: FILE HOST
my $DEFAULT_SUBJECT= 'Results for FILE on host: HOST';

## The possible types of levels in Postgres logs
my %level = map { $_ => 1 } qw{PANIC FATAL ERROR WARNING NOTICE LOG INFO};
for (1..5) { $level{"DEBUG$_"} = 1; }
my $levels = join '|' => keys %level;
my $levelre = qr{(?:$levels)};

## We can define custom types, e.g. "duration" that get printed differently
my $custom_type = 'normal';

## Read in the the options
my ($verbose,$quiet,$debug,$dryrun,$reset,$limit,$rewind,$version) = (0,0,0,0,0,0,0,0);
my ($custom_offset,$custom_duration,$custom_file,$nomail,$flatten) = (-1,-1,'',0,1);
my ($timewarp,$pgmode) = (0,1);
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

## Read in and parse the config file
my %opt = (DEFAULT => { email => []});
my $curr = 'DEFAULT';
open my $c, '<', $configfile or die qq{Could not open "$configfile": $!\n};
my $in_standard_comments = 1;
my (@comment, %itemcomment);
while (<$c>) {
    if ($in_standard_comments) {
        next if /^## Config file for/;
        next if /^## This file is automatically updated/;
        next if /^## Last updated:/;
        next if /^\s*$/;
        $in_standard_comments = 0;
    }

    if (/^\s*#/) {
        ## Assume that this is an important comment, and store it for the next real word
        push @comment => $_;
        next;
    }

    ## Store comments as needed
    if (@comment and m{^(\w+):}) {
        chomp;
        for my $c (@comment) {
            push @{$itemcomment{$1}} => $c;
            push @{$itemcomment{$_}} => $c;
        }
        undef @comment;
    }

    ## Which files to check
    if (/^FILE:\s*(.+?)\s*$/) {
        $curr = $1;
        ## Transform if needed
        my $filename = $curr;
        if ($curr =~ /%/) {
            eval { ## no critic (RequireCheckingReturnValueOfEval)
                require POSIX;
            };

            $@ and die qq{Cannot use strftime formatting without the Perl POSIX module!\n};
            my @ltime = localtime(time + $timewarp);
            $filename = POSIX::strftime($curr, @ltime); ## no critic (ProhibitCallsToUnexportedSubs)
        }

        if ($filename =~ /LATEST/) {
            $filename = find_latest_logfile($filename);
        }

        if ($custom_file) {
            if ($custom_file =~ m{/}) {
                $filename = $custom_file;
            }
            else {
                my $dir = dirname($filename);
                $filename = "$dir/$custom_file";
            }
        }

        $opt{$curr} =
            {
             email         => [],
             exclude       => [],
             include       => [],
             mailsubject   => $DEFAULT_SUBJECT,
             customsubject => 0,
             filename      => $filename,
             };
    }
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
        $opt{DEFAULT}{pgformat} = $pgformat = $1;
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
            $custom_duration = $opt{DEFAULT}{duration} = $1;
        }
    }
    ## Force line number lookup on or off
    elsif (/^FIND_LINE_NUMBER:\s*(\d+)/) {
        $find_line_number = $opt{DEFAULT}{find_line_number} = $1;
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
    elsif (/^MAILSUBJECT:\s*(.+)/) { ## Want whitespace
        $opt{$curr}{mailsubject} = $1;
        $opt{$curr}{customsubject} = 1;
    }
}
close $c or die qq{Could not close "$configfile": $!\n};

for (keys %opt) {
    delete $opt{$_} if /^_/;
}
$debug and warn Dumper \%opt;

## Any changes that need saving?
my $save = 0;

my ($fh, $tempfile);
## Process each file in turn:

my ($exclude, $include, %current_pid_num, $toolarge);
my (%find, %similar, $lastpid, %sorthelp);

EACHFILE: for my $file (sort keys %opt) {

    undef %find;
    undef %similar;
    undef %current_pid_num;
    my %pidline;

    next EACHFILE if $file eq 'DEFAULT';

    my $filename = $opt{$file}{filename};
    $verbose and warn "Checking file $filename\n";

    my $rotatedfile = 0;

    ## Come back to this point if we just finished with a rotated file
  ROTATED:

    if ($rotatedfile) {
        $rotatedfile = 0;
        ## Reset the name back to the current one
        $filename = $opt{$file}{filename};
        ## If we just finished looking at the old rotated file, offset must be zero
        $opt{$file}{offset} = 0;
    }
    ## If the last file we checked is not the current one, go back and finish up that one
    elsif (exists $opt{$file}{lastfile} and $opt{$file}{lastfile} ne $filename and ! $custom_file) {
        $verbose and warn "  Logfile was rotated, checking end of last file first.\n";
        $filename = $opt{$file}{lastfile};
        $rotatedfile = 1;
    }

    my $fileokay = 1;

    ## Does it exist?
    if (! -e $filename) {
        $quiet or warn qq{WARNING! Skipping non-existent file "$filename"\n};
        $fileokay = 0;
    }
    elsif (! -f $filename) {
        warn qq{WARNING! Skipping non-file "$filename"\n};
        $fileokay = 0;
    }

    if (!$fileokay) {
        if ($rotatedfile) {
            ## Failed to find the last file, so continue on with the real one
            goto ROTATED;
        }
        next EACHFILE;
    }

    my $size = -s $filename;

    ## Determine the new offset
    $opt{$file}{offset} ||= 0;
    my $offset = $opt{$file}{offset};

    ## Allow the offset to be changed on the command line
    if ($custom_offset>=0) {
        $offset = $custom_offset;
    }
    elsif ($custom_offset < -1) {
        $offset = $size + $custom_offset;
        $offset = 0 if $offset < 0;
    }

    my $maxsize = exists $opt{$file}{maxsize} ? $opt{$file}{maxsize} : $MAXSIZE;
    $verbose and warn "  Offset: $offset Size: $size Maxsize: $maxsize\n";
    if ($reset) {
        $offset = $size;
        $verbose and warn "  Resetting offset to $offset\n";
    }

    ## The file may have shrunk due to a logrotate
    if ($offset > $size) {
        $verbose and warn "  File has shrunk - resetting offset to 0\n";
        $offset = 0;
    }

    ## Read in the lines if necessary
  TAILIT: {
        $toolarge = '';
        if ($offset < $size) {

            if ($maxsize and $size - $offset > $maxsize and $custom_offset < 0) {
                warn "  SIZE TOO BIG (size=$size, offset=$offset): resetting to last $maxsize bytes\n";
                $toolarge = "File too large: only read last $maxsize bytes (size=$size, offset=$offset)";
                $offset = $size - $maxsize;
            }

            my $original_offset = $offset;
            $offset = 10 if $offset < 10; ## We go back before the newline
            open $fh, '<', $filename or die qq{Could not open "$filename": $!\n};
            seek $fh, $offset-10, 0;
            if ($rewind) {
                seek $fh, -$rewind, 1;
            }

            ## Figure out what line we are on (roughly)
            my $pos = tell $fh;
            my $newlines = 0;
            if ($pos > 1 and $find_line_number) {
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
                seek $fh, 0, $pos;
            }

            ## Build an exclusion regex for this file
            $exclude = '';
            for my $ex (@{$opt{$file}{exclude}}) {
                $debug and warn "  Adding exclusion: $ex\n";
                my $regex = qr{$ex};
                $exclude .= "$regex|";
            }
            $exclude =~ s/\|$//;
            $verbose and $exclude and warn "  Exclusion: $exclude\n";

            ## Build an inclusion regex for this file
            $include = '';
            for my $in (@{$opt{$file}{include}}) {
                $debug and warn "  Adding inclusion: $in\n";
                my $regex = qr{$in};
                $include .= "$regex|";
            }
            $include =~ s/\|$//;
            $verbose and $include and warn "  Inclusion: $include\n";

            ## Discard previous line
            $offset > 10 and <$fh>;
            my $count = 0;

            ## Postgres-specific multi-line grabbing stuff:
            my ($pgpid, $pgnum, $pgsub);

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
                                or (/ LOG:  statement: /o and $lastline !~ /  statement: |FATAL|PANIC/)) {
                                $pgnum = $current_pid_num{$pgpid} + 1;
                            }
                            ## Is this a new entry for this PID? If so, process the old.
                            if ($pgnum <= 1) {
                                $count += process_line($pidline{$pgpid});
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
                $count += process_line($_, $. + $newlines);

            } ## end of each line in the file

            seek $fh, 0, 1;
            $offset = tell $fh;
            close $fh or die qq{Could not close "$filename": $!\n};

            ## Now add in any pids that have not been processed yet
            for my $pid (keys %pidline) {
                $count += process_line($pidline{$pid});
            }

            if (!$count) {
                $verbose and warn "  No new lines found in file\n";
                last TAILIT;
            }
            $verbose and warn "  Lines found: $count\n";

            if ($rotatedfile) {
                $verbose and warn "  Finished with previous file $filename, resuming scan of $opt{$file}{filename}\n";
                $opt{$file}{lastfilecount} = $count;
                goto ROTATED;
            }

            ## Create the mail message
            ($fh, $tempfile) = tempfile('tnmXXXXXXXX', SUFFIX => '.tnm');

            if ($dryrun) {
                close $fh or warn 'Could not close filehandle';
                $fh = \*STDOUT;
            }

            ## Subject with replaced keywords:
            my $subject = $opt{$file}{mailsubject};
            $subject =~ s/FILE/$filename/g;
            $subject =~ s/HOST/$hostname/g;
            print {$fh} "Subject: $subject\n";

            ## Discourage vacation programs from replying
            print {$fh} "Auto-Submitted: auto-generated\n";
            print {$fh} "Precedence: bulk\n";

            ## Some minor help with debugging
            print {$fh} "X-TNM-VERSION: $VERSION\n";

            ## Fill out the "To:" fields
            for my $email (@{$opt{DEFAULT}{email}}, @{$opt{$file}{email}}) {
                print {$fh} "To: $email\n";
            }

            ## Custom From:
            my $from_addr = $opt{DEFAULT}{from} || '';
            if ($from_addr ne '') {
                print {$fh} "From: $from_addr\n";
                $MAILCOM .= " -f $from_addr";
            }
            ## End header section
            print {$fh} "\n";

            ## Indicate that we looked at multiple files
            if ($opt{$file}{lastfilecount}) {
                print {$fh} "Matches from $opt{$file}{lastfile}: $opt{$file}{lastfilecount}\n";
            }
            print {$fh} "Matches from $filename: $count\n";
            my $now = scalar localtime;
            print {$fh} "Date: $now\n";
            print {$fh} "Host: $hostname\n";

            ## Any other interesting things about this run
            if ($custom_duration >= 0) {
               print {$fh} "Minimum duration: $custom_duration\n";
            }
            if ($toolarge) {
                print {$fh} "$toolarge\n";
            }

            ## The meat of the message
            lines_of_interest($fh, $original_offset);

            print {$fh} "\n";
            close $fh or die qq{Could not close "$tempfile": $!\n};

            my $emails = join ' ' => @{$opt{DEFAULT}{email}}, @{$opt{$file}{email}};
            $verbose and warn "  Sending mail to: $emails\n";
            my $COM = qq{$MAILCOM $emails < $tempfile};
            if ($dryrun or $nomail) {
                warn "  DRYRUN: $COM\n";
            }
            else {
                system $COM;
            }
            unlink $tempfile;
        }
    } ## end of TAILIT

    ## If offset has changed, save it
    if (!$rotatedfile and $offset != $opt{$file}{offset}) {
        $opt{$file}{offset} = $offset;
        $save++;
        $verbose and warn "  Setting offset to $offset\n";
    }

} ## end each file to check

if ($save and !$dryrun) {
    $verbose and warn "Saving new config file (save=$save)\n";
    open $fh, '>', $configfile or die qq{Could not write "$configfile": $!\n};
    my $oldselect = select $fh;
    my $now = localtime;
    print qq{
## Config file for the $me program
## This file is automatically updated
## Last updated: $now
};

    for my $item (qw/ email from pgformat type duration find_line_number /) {
        next if ! exists $opt{DEFAULT}{$item};
        next if $item eq 'duration' and $custom_duration < 0;
        add_comments(uc $item);
        if (ref $opt{DEFAULT}{$item} eq 'ARRAY') {
            for my $itemz (@{$opt{DEFAULT}{$item}}) {
                printf "%s: %s\n", uc $item, $itemz;
            }
        }
        else {
            printf "%s: %s\n", uc $item, $opt{DEFAULT}{$item};
        }
    }

    for my $file (sort keys %opt) {
        next if $file eq 'DEFAULT';
        print "\n";
        add_comments('FILE');
        print "FILE: $file\n";
        print "LASTFILE: $opt{$file}{filename}\n";
        print "OFFSET: $opt{$file}{offset}\n";
        printf "MAXSIZE: %d\n",
            exists $opt{$file}{maxsize} ? $opt{$file}{maxsize} : $MAXSIZE;
        for my $email (@{$opt{$file}{email}}) {
            add_comments("EMAIL: $email");
            print "EMAIL: $email\n";
        }
        if ($opt{$file}{customsubject}) {
            add_comments('MAILSUBJECT');
            print "MAILSUBJECT: $opt{$file}{mailsubject}\n";
        }
        for my $include (@{$opt{$file}{include}}) {
            add_comments("INCLUDE: $include");
            print "INCLUDE: $include\n";
        }
        for my $exclude (@{$opt{$file}{exclude}}) {
            add_comments("EXCLUDE: $exclude");
            print "EXCLUDE: $exclude\n";
        }

        print "\n";
    }
    select $oldselect;
    close $fh or die qq{Could not close "$configfile": $!\n};
}

exit;

sub find_latest_logfile {

    my $file = shift;
    $file =~ s/LATEST/*/;
    my $latest = qx{ls -rt1 $file | tail -1};
    chomp $latest;
    return $latest;

} ## end of find_latest_logfile

sub process_line {

    ## We've got a complete statement, so do something with it!
    ## If it matches, we'll either put into %find directly, or store in %similar

    my $arg = shift;

    ## What line was this string first spotted at?
    my $line = shift || 0;

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

    ## Compress all whitespace
    $string =~ s/\s+/ /g;

    ## If not in Postgres mode, we avoid all the mangling below
    if (!$pgmode) {
        $find{$line} = $string;
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
            ## See if this is the new earliest one
            if ($line < $similar{$body}{earliestline}) {
                ## Remove the old winner
                delete $find{$similar{$body}{earliestline}};
                ## Replace with our new one:
                $find{$line} = $similar{$body};
                ## Set the new information:
                $similar{$body}{earliest} = $string;
                $similar{$body}{earliestnonflat} = $nonflat;
                $similar{$body}{earliestline} = $line;
            }
            ## See if this is the new latest one
            elsif ($line > $similar{$body}{latestline}) {
                ## Set the new information:
                $similar{$body}{latest} = $string;
                $similar{$body}{latestline} = $line;
            }

            ## Increment the count
            $similar{$body}{count}++;
        }

        if (!$seenit) {
            ## Store as the earliest and latest version we've seen
            $similar{$body}{earliest} = $similar{$body}{latest} = $string;
            $similar{$body}{earliestnonflat} = $nonflat;
            $similar{$body}{earliestline} = $similar{$body}{latestline} = $line;
            ## Start counting these items
            $similar{$body}{count} = 1;
            ## Store this away for eventual output
            $find{$line} = $similar{$body};
        }
    }
    else {
        $find{$line} = $string;
    }

    return 1;

} ## end of process_line


sub lines_of_interest {

    ## Given a file handle, print all our current lines to it

    my ($lfh,$loffset) = @_;

    my $oldselect = select $lfh;

    undef %sorthelp;
    sub sortsub {
        if ($custom_type eq 'duration') {
            if (! exists $sorthelp{$find{$a}}) {
                $sorthelp{$find{$a}} =
                    $find{$a}{earliest} =~ /duration: (\d+\.\d+)/ ? $1 : 0;
            }
            if (! exists $sorthelp{$find{$b}}) {
                $sorthelp{$find{$b}} =
                    $find{$b}{earliest} =~ /duration: (\d+\.\d+)/ ? $1 : 0;
            }
            return ($sorthelp{$find{$b}} <=> $sorthelp{$find{$a}} or $a <=> $b);
        }

        return $a <=> $b;

    }

    my $count = 1;
    for my $line (sort sortsub keys %find) {
        print "\n[$count]";
        $count++;
        if (ref $find{$line} eq 'HASH') {
            ## If there is only one, we just print as a normal line
            if ($find{$line}{count} <= 1) {
                $find{$line} = $find{$line}{earliestnonflat};
                ## pass through to below
            }
            else {
                my $firstline = $find{$line}{earliestline};
                my $lastline = $find{$line}{latestline};
                print " Between lines $firstline and $lastline, occurs $find{$line}{count} times.";
                ## If we can, show just the interesting part
                my $latest = $find{$line}{latest};
                my $earliest = $find{$line}{earliest};
                if ($pgmode == 1 and $earliest =~ s/$pgpiddatere(.+)/$2/) {
                    my $headstart = $1;
                    $latest =~ /$pgpiddatere/ or die "Latest did not match?!\n";
                    my $headend = $1; ## no critic (ProhibitCaptureWithoutTest)
                    print "\nFirst: $headstart\nLast:  $headend\n";
                    print "Statement: $earliest\n";
                }
                else {
                    print " Earliest and latest:\n";
                    print "$earliest\n$latest\n";
                }
                next;
            }
        }
        ($find_line_number or !$loffset) and print " (from line $line)\n";
        print "$find{$line}\n";
    }

    select $oldselect;

    return;

} ## end of lines_of_interest

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

