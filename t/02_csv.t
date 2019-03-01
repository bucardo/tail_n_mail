#!perl

use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use Test::More;

if (!eval { require Text::CSV; 1 } and !eval { require Text::CSV_XS; 1}) {
	plan skip_all => 'Could not find Text::CSV or Text::CSV_XS';
}
else {
	plan tests => 14;
}

use vars qw{ $info $t };

sub run {

	my $conf = shift or die "Must supply a config file!\n";

	my $options = shift || '';

	my $COM = "perl tail_n_mail --dryrun --no-tailnmailrc $conf $options";

	my $result = '';
	eval {
		$result = qx{$COM 2>&1};
	};
	return $@ ? "OOPS: $@\n" : $result;
}

my $num = 2;

$info = run('t/config/config2.txt');

my $start = substr($info,0,300);
$t = qq{Test config $num gives correct subject line};
my $host = qx{hostname};
chomp $host;
like ($start, qr{^Subject: Acme $host Postgres errors 45 : 60\n}, $t);

$t = qq{Test config $num inserts a 'Date' line with newlines before it};
like ($start, qr{\n\n\Date: \w\w\w \w\w\w [\d ]\d}, $t);

$t = qq{Test config $num inserts a 'Host' line};
like ($start, qr{\nHost: \w}, $t);

$t = qq{Test config $num inserts a 'Unique items' line};
like ($start, qr{\nUnique items: 45\n}, $t);

$t = qq{Test config $num inserts a 'Matches from' line};
like ($start, qr{\nMatches from t/logs/testlog2.csv: 60\n}, $t);

$t = qq{Test config $num gives a 'DRYRUN' line};
like ($info, qr{DRYRUN: }, $t);

$t = qq{Test config $num gives correct first item match};
$start = substr($info,0,700);
like ($start, qr{\n\Q[1] (between lines 8,399 and 8,435, occurs 5 times)}, $t);

$t = qq{Test config $num gives correct first item "First" timestamp};
like ($start, qr{\n\QFirst: 2010-12-23 11:28:07.653 EST [7698]\E\n}, $t);

$t = qq{Test config $num gives correct first item "Last" timestamp};
like ($start, qr{\n\QLast:  2010-12-23 11:28:07.670 EST [7698]\E\n}, $t);

$t = qq{Test config $num gives correct normalized output};
like ($start, qr{\nERROR: invalid input syntax for type point: "\?"
STATEMENT: INSERT INTO dbd_pg_test_geom\(xpoint\) VALUES \(\?\)}, $t);

$t = qq{Test config $num gives correct literal output};
like ($start, qr{\n\-\nERROR: invalid input syntax for type point: "123,abc"
STATEMENT: INSERT INTO dbd_pg_test_geom\(xpoint\) VALUES \(\$1\)}, $t);

## Third match is a simple COPY error with a CONTEXT
$t = qq{Test config $num gives correct third match};
$start = substr($info,950,350);
like ($start, qr{\Q
[3] (between lines 7,365 and 7,374, occurs 3 times)
First: 2010-12-23 11:27:56.010 EST [7679]
Last:  2010-12-23 11:27:56.013 EST [7679]
ERROR: COPY from stdin failed: COPY terminated by new PQexec
CONTEXT: COPY dbd_pg_test4, line 1
STATEMENT: COPY dbd_pg_test4 FROM STDIN\E}, $t);

$t = qq{Test config $num gives correct fourth match};
$start = substr($info,1220,300);
like ($start, qr{\Q
[4] (between lines 737 and 738, occurs 2 times)
First: 2010-12-23 11:27:53.100 EST [7649]
Last:  2010-12-23 11:27:53.101 EST [7649]
ERROR: syntax error at or near "Testing"
STATEMENT: Testing the ShowErrorStatement attribute
\E}, $t);

## Sixth match has no normalization
$t = qq{Test config $num gives correct sixth match};
$start = substr($info,1600,500);
like ($start, qr{\Q
[6] (between lines 6,607 and 6,907, occurs 2 times)
First: 2010-12-23 11:27:55.046 EST [7661]
Last:  2010-12-23 11:27:55.307 EST [7665]
ERROR: column "dbdpg_throws_an_error" does not exist
STATEMENT: SELECT dbdpg_throws_an_error
\E}, $t);

exit;
