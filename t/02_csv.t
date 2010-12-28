#!perl

use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use Test::More tests => 14;

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

my $num = 2;

$info = run('t/config/config2.txt');
my $start = substr($info,0,250);
$t = qq{Test config $num gives correct subject line};
like ($start, qr{^Subject: Acme core Postgres errors 50 : 60\n}, $t);

$t = qq{Test config $num inserts a 'Date' line with newlines before it};
like ($start, qr{\n\n\Date: \w\w\w \w\w\w \d\d}, $t);

$t = qq{Test config $num inserts a 'Host' line};
like ($start, qr{\nHost: \w}, $t);

$t = qq{Test config $num inserts a 'Unique items' line};
like ($start, qr{\nUnique items: 50\n}, $t);

$t = qq{Test config $num inserts a 'Matches from' line};
like ($start, qr{\nMatches from t/logs/testlog2.csv: 60\n}, $t);

$t = qq{Test config $num gives a 'DRYRUN' line};
like ($info, qr{DRYRUN: }, $t);

$t = qq{Test config $num gives correct first item match};
$start = substr($info,0,600);
like ($start, qr{\n\Q[1] (between lines 8,450 and 8,486, occurs 5 times)}, $t);

$t = qq{Test config $num gives correct first item "First" timestamp};
like ($start, qr{\n\QFirst: 2010-12-23 11:28:07.675 EST [7698]\E\n}, $t);

$t = qq{Test config $num gives correct first item "Last" timestamp};
like ($start, qr{\n\QLast:  2010-12-23 11:28:07.686 EST [7698]\E\n}, $t);

$t = qq{Test config $num gives correct normalized output};
like ($start, qr{\nERROR: type "line" not yet implemented
\QSTATEMENT: INSERT INTO dbd_pg_test_geom(xline) VALUES (?)\E\n\-\n}, $t);

$t = qq{Test config $num gives correct literal output};
like ($start, qr{\n\-\nERROR: type "line" not yet implemented
\QSTATEMENT: INSERT INTO dbd_pg_test_geom(xline) VALUES (\E\$1\)}, $t);

## Second match is a simple COPY error with a CONTEXT
$t = qq{Test config $num gives correct second match};
$start = substr($info,550,300);
like ($start, qr{\Q
[2] (between lines 7,365 and 7,374, occurs 3 times)
First: 2010-12-23 11:27:56.010 EST [7679]
Last:  2010-12-23 11:27:56.013 EST [7679]
ERROR: COPY from stdin failed: COPY terminated by new PQexec
CONTEXT: COPY dbd_pg_test4, line 1
STATEMENT: COPY dbd_pg_test4 FROM STDIN\E}, $t);

$t = qq{Test config $num gives correct third match};
$start = substr($info,500,1000);
like ($start, qr{\Q
[3] (between lines 737 and 738, occurs 2 times)
First: 2010-12-23 11:27:53.100 EST [7649]
Last:  2010-12-23 11:27:53.101 EST [7649]
ERROR: syntax error at or near "Testing"
STATEMENT: Testing the ShowErrorStatement attribute
\E}, $t);

## Fifth match has no normalization
$t = qq{Test config $num gives correct fifth match};
$start = substr($info,1350,500);
like ($start, qr{\Q
[5] (between lines 6,607 and 6,907, occurs 2 times)
First: 2010-12-23 11:27:55.046 EST [7661]
Last:  2010-12-23 11:27:55.307 EST [7665]
ERROR: column "dbdpg_throws_an_error" does not exist
STATEMENT: SELECT dbdpg_throws_an_error
\E}, $t);

exit;
