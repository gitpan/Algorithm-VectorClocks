use strict;
use warnings;
use Test::More tests => 29;

use Algorithm::VectorClocks;
use JSON::Any;

my $json = JSON::Any->new;

my $vc_a = Algorithm::VectorClocks->new;
ok $vc_a->id;
   $vc_a->id('A');
is $vc_a->id, 'A';
is_deeply $vc_a->clocks, {};

my $vc_b = Algorithm::VectorClocks->new('B');
my $vc_c = Algorithm::VectorClocks->new('C');

### in node A ###

$vc_a->increment;
is_deeply $vc_a->clocks, { A => 1 };

my $serialized_a = $vc_a->serialize;
is_deeply $json->jsonToObj($serialized_a), { id => 'A', clocks => { A => 1 } };

### in node B ###

$vc_b->merge($serialized_a);
is_deeply $vc_b->clocks, { A => 1 };

$vc_b++;
is_deeply $vc_b->clocks, { A => 1, B => 1 };

my $serialized_b = "$vc_b";

### in node A ###

$vc_a += $serialized_b;
is_deeply $vc_a->clocks, { A => 1, B => 1 };

$vc_a++;
is_deeply $vc_a->clocks, { A => 2, B => 1 };

$serialized_a = "$vc_a";

ok   $vc_b == $serialized_b;
ok   $vc_b eq $serialized_b;
ok !($vc_b != $serialized_b);
ok !($vc_b ne $serialized_b);

### in node C ###

ok !$vc_b->equal($serialized_a);
ok  $vc_b->not_equal($serialized_a);

$vc_c += $serialized_b;
is_deeply $vc_c->clocks, { A => 1, B => 1 };

$vc_c++;
is_deeply $vc_c->clocks, { A => 1, B => 1, C => 1 };

my $serialized_c = "$vc_c";

### in client ###

my @vcs = order_vector_clocks($serialized_a);
is @vcs, 1;
is $vcs[0]->id, 'A';

@vcs = order_vector_clocks($serialized_a, $serialized_a);
is @vcs, 2;
is $vcs[0]->id, 'A';
is $vcs[1]->id, 'A';

@vcs = order_vector_clocks($serialized_c, $serialized_a);
is @vcs, 1;
is $vcs[0][0]->id, 'C';
is $vcs[0][1]->id, 'A';

@vcs = order_vector_clocks($serialized_a, $serialized_b, $serialized_c);
is @vcs, 2;
is $vcs[0][0]->id, 'A';
is $vcs[0][1]->id, 'C';
is $vcs[1]->id, 'B';
