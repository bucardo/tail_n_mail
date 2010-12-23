#!perl

use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use Test::More tests => 19;

use vars qw{ $info $t };

sub run {

	my $conf = shift or die "Must supply a config file!\n";

	my $options = shift || '';

	my $COM = "perl tail_n_mail --dryrun  $conf $options";

	my $result = '';
	eval {
		$result = qx{$COM 2>&1};
	};
	return $@ ? "OOPS: $@\n" : $result;
}

$t = q{Running with non-existent config file gives an error};
$info = run('t/nosuch.config');
like ($info, qr{Could not open}, $t);

$t = q{Program outputs version information correctly};
my $verinfo = run('--version');
like ($verinfo, qr{^tail_n_mail version \d+\.\d+\.\d+}, $t);
$verinfo =~ /(\d+\.\d+\.\d+)/ or die;
my $ver = $1;

$info = run('t/config/config1.txt');
my $start = substr($info,0,250);
$t = q{Test config 1 gives correct subject line};
like ($start, qr{^Subject: Acme core Postgres errors 50 : 60\n}, $t);

$t = q{Test config 1 gives correct bulk email headers};
like ($start, qr{\nAuto-Submitted: auto-generated\nPrecedence: bulk\n}, $t);

$t = q{Test config 1 gives correct version header};
like ($start, qr{\nX-TNM-VERSION: $ver\n}, $t);

$t = q{Test config 1 inserts a 'To' header};
like ($start, qr{\nTo: someone\@example.com\n}, $t);

$t = q{Test config 1 inserts a 'Date' line with newlines before it};
like ($start, qr{\n\n\Date: \w\w\w \w\w\w \d\d}, $t);

$t = q{Test config 1 inserts a 'Host' line};
like ($start, qr{\nHost: \w}, $t);

$t = q{Test config 1 inserts a 'Unique items' line};
like ($start, qr{\nUnique items: 50\n}, $t);

$t = q{Test config 1 inserts a 'Matches from' line};
like ($start, qr{\nMatches from t/logs/testlog1.txt: 60\n}, $t);

$t = q{Test config 1 gives a 'DRYRUN' line};
like ($info, qr{DRYRUN: }, $t);

$t = q{Test config 1 gives correct first item match};
$start = substr($info,0,600);
like ($start, qr{\n\Q[1] (between lines 9,603 and 9,647, occurs 5 times)}, $t);

$t = q{Test config 1 gives correct first item "First" timestamp};
like ($start, qr{\n\QFirst: 2010-12-22 19:17:53 EST [29929]\E\n}, $t);

$t = q{Test config 1 gives correct first item "Last" timestamp};
like ($start, qr{\n\QLast:  2010-12-22 19:17:53 EST [29929]\E\n}, $t);

$t = q{Test config 1 gives correct normalized output};
like ($start, qr{\nERROR: type "line" not yet implemented
\QSTATEMENT: INSERT INTO dbd_pg_test_geom(xline) VALUES (?)\E\n\-\n}, $t);

$t = q{Test config 1 gives correct literal output};
like ($start, qr{\n\-\nERROR: type "line" not yet implemented
\QSTATEMENT: INSERT INTO dbd_pg_test_geom(xline) VALUES (\E\$1\)}, $t);

## Second match is a simple COPY error with a CONTEXT
$t = q{Test config 1 gives correct second match};
$start = substr($info,500,800);
like ($start, qr{\Q
[2] (between lines 8,187 and 8,200, occurs 3 times)
First: 2010-12-22 19:17:41 EST [29896]
Last:  2010-12-22 19:17:41 EST [29896]
ERROR: COPY from stdin failed: COPY terminated by new PQexec
CONTEXT: COPY dbd_pg_test4, line 1
STATEMENT: COPY dbd_pg_test4 FROM STDIN
\E}, $t);

## Third match has a "character X" substitution
$t = q{Test config 1 gives correct third match};
$start = substr($info,500,1000);
like ($start, qr{\Q
[3] (between lines 1,013 and 1,015, occurs 2 times)
First: 2010-12-22 19:17:38 EST [29867]
Last:  2010-12-22 19:17:38 EST [29867]
ERROR: syntax error at or near "?" at character ?
STATEMENT: Testing the ShowErrorStatement attribute
-
ERROR: syntax error at or near "Testing" at character 1
STATEMENT: Testing the ShowErrorStatement attribute
\E}, $t);


## Fifth match has no normalization
$t = q{Test config 1 gives correct third match};
$start = substr($info,1500,300);
like ($start, qr{\Q
[5] (between lines 7,093 and 7,526, occurs 2 times)
First: 2010-12-22 19:17:40 EST [29878]
Last:  2010-12-22 19:17:40 EST [29882]
ERROR: column "dbdpg_throws_an_error" does not exist at character 8
STATEMENT: SELECT dbdpg_throws_an_error
\E}, $t);

exit;
