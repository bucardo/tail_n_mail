#!perl

## Make sure the version number is consistent in all places
## Make sure no tabs have crept into the files
## Make sure the git tags are correct

use 5.006;
use strict;
use warnings;
use Data::Dumper;
use Test::More;
use lib 't','.';

if (! $ENV{RELEASE_TESTING}) {
	plan (skip_all =>  'Test skipped unless environment variable RELEASE_TESTING is set');
}

plan tests => 3;

my %v;
my $vre = qr{(\d+\.\d+\.\d+\_?\d*)};

my $file = 'tail_n_mail';
open my $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	push @{$v{$file}} => [$1,$.] if /VERSION = '$vre'/;
}
close $fh or warn qq{Could not close "$file": $!\n};

$file = 'Changes';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
	if (/^$vre/) {
		push @{$v{$file}} => [$1,$.];
		last;
	}
}
close $fh or warn qq{Could not close "$file": $!\n};

my $good = 1;
my $lastver;
for my $filename (keys %v) {
	for my $glob (@{$v{$filename}}) {
		my ($ver,$line) = @$glob;
		if (! defined $lastver) {
			$lastver = $ver;
		}
		elsif ($ver ne $lastver) {
			$good = 0;
		}
	}
}

if ($good) {
	pass "All version numbers are the same ($lastver)";
}
else {
	fail 'All version numbers were not the same!';
	for my $filename (sort keys %v) {
		for my $glob (@{$v{$filename}}) {
			my ($ver,$line) = @$glob;
			diag "File: $filename. Line: $line. Version: $ver\n";
		}
	}
}

## Make sure all tabs and invalid characters are removed
for my $file (qw/ tail_n_mail /) {
	open $fh, '<', $file or die qq{Could not open "$file": $!\n};
	$good = 1;
	while (<$fh>) {
		if (/\t/) {
			diag "Found a tab at line $. of $file\n";
			$good = 0;
		}
		if (! /^[\S ]*/) {
			diag "Invalid character at line $. of $file: $_\n";
			$good = 0;
		}
	}
	close $fh or warn qq{Could not close "$file": $!\n};

	if ($good) {
		pass "The file $file has no tabs or unusual characters";
	}
	else {
		fail "The file $file did not pass inspection!";
	}
}

## We must have a git tag for this version
my $COM = "git tag -v $lastver 2>&1";

my $info = qx{$COM};

if ($info =~ /error/) {
	fail "No such git tag: $lastver\n";
}
elsif ($info !~ /Good signature/) {
	fail "No signatiure found for git tag $lastver";
}
else {
	pass "Valid signed git tag found for version $lastver";
}

exit;
