package Algorithm::VectorClocks;

use warnings;
use strict;
use Carp;

use version; our $VERSION = qv('0.0.1');

use JSON::Any;
use List::MoreUtils qw(uniq);
use List::Util qw(max);
use Perl6::Export::Attrs;
use Sys::Hostname;

use overload (
    '""' => \&serialize,
    '++' => \&increment,
    '+=' => \&merge,
    '==' => \&equal,
    'eq' => \&equal,
    '!=' => \&not_equal,
    'ne' => \&not_equal,
    fallback => undef,
);

use base qw(Class::Accessor::Fast);

__PACKAGE__->mk_accessors(qw(id clocks));

my $json = JSON::Any->new;

sub new {
    my $class = shift;
    my($arg) = @_;
    my $self = !$arg || $arg !~ /^\{/ ? { id => ($arg || hostname), clocks => {} }
             : !ref $arg              ? $json->jsonToObj($arg)
             :                          $arg;
    bless $self, $class;
}

sub serialize {
    my $self = shift;
    $json->objToJson({ id => $self->id, clocks => $self->clocks });
}

sub increment {
    my $self = shift;
    $self->clocks->{ $self->id }++; # increment its own clock
    $self;
}

sub merge {
    my $self = shift;
    my($other) = @_;
    $other = __PACKAGE__->new($other);
    my @ids = _list_ids($self, $other);
    for my $id (@ids) {
        $self->clocks->{$id}
            = max( ($self->clocks->{$id} || 0), ($other->clocks->{$id} || 0) );
    }
    $self;
}

sub equal {
    my @vcs = @_;
    $_ = __PACKAGE__->new($_) for @vcs;
    my @ids = _list_ids(@vcs);
    for my $id (@ids) {
        return 0
            unless ($vcs[0]->clocks->{$id} || 0) == ($vcs[1]->clocks->{$id} || 0);
    }
    return 1;
}

sub not_equal { !equal(@_) }

sub order_vector_clocks :Export(:DEFAULT) {
    my @vcs = @_;
    $_ = __PACKAGE__->new($_) for @vcs;
    @vcs = sort { _compare($b, $a) } @vcs;
    _pack_independent_vector_clocks(@vcs);
}

sub _pack_independent_vector_clocks {
    my @vcs = @_;
    my @ret;
    my $i = 0;
    while ($i < @vcs) {
        my @suspects = (
            $vcs[$i],
            (grep { _compare($vcs[$i], $_) == 0 } @vcs[($i+1)..$#vcs])
        );
        push @ret, _are_independent(@suspects) ? \@suspects : @suspects;
        $i += @suspects;
    }
    @ret;
}

sub _are_independent {
    my @vcs = @_;
    for (my $j = 0; $j < @vcs; $j++) {
        for (my $k = $j+1; $k < @vcs; $k++) {
            return 1 if $vcs[$j]->_is_independent($vcs[$k]);
        }
    }
}

sub _is_independent {
    my $self = shift;
    my($other) = @_;
    $other = __PACKAGE__->new($other);
    my @ids = _list_ids($self, $other);
    my $res = 0;
    for my $id (@ids) {
        my $r = ($self->clocks->{$id} || 0) - ($other->clocks->{$id} || 0);
        if    ($res == 0 ) { $res = $r }
        elsif ($r   == 0 ) {           }
        elsif ($res != $r) { return 1  }
    }
    0;
}

sub _compare {
    my $self = shift;
    my($other) = @_;
    $other = __PACKAGE__->new($other);
    my @ids = _list_ids($self, $other);
    my $res = 0;
    for my $id (@ids) {
        my $r = ($self->clocks->{$id} || 0) - ($other->clocks->{$id} || 0);
        if    ($res == 0 ) { $res = $r }
        elsif ($r   == 0 ) {           }
        elsif ($res != $r) { return 0  } # independent
    }
    $res;
}

sub _list_ids { uniq map { keys %{ $_->clocks } } @_ }

1; # Magic true value required at end of module
__END__

=head1 NAME

Algorithm::VectorClocks - Generating a partial ordering of events in a distributed system


=head1 SYNOPSIS

    use Algorithm::VectorClocks;

    ### in node A ###

    my $vc_a = Algorithm::VectorClocks->new('A');

    $vc_a->increment; # same as $vc_a++

    my $serialized_a = $vc_a->serialize; # same as "$vc_a"

    # send a message with $serialized_a to node B

    ### in node B ###

    my $vc_b = Algorithm::VectorClocks->new('B');

    # receive the message with $serialized_a from node A

    $vc_b->merge($serialized_a); # same as $vc_b += $serialized_a

    $vc_b->increment;

    my $serialized_b = $vc_b->serialize;

    ### in client ###

    # retrieves $serialized_a and $serialized_b

    my @vcs = order_vector_clocks($serialized_a, $serialized_b);
    $vcs[0]->id; # 'B' is the latest
    $vcs[1]->id; # 'A'


=head1 DESCRIPTION

Description, shamelessly stolen from Wikipedia:

    Vector Clocks is an algorithm for generating a partial ordering of
    events in a distributed system. Just as in Lamport timestamps,
    interprocess messages contain the state of the sending process's
    logical clock. Vector clock of a system of N processes is an array
    of N logical clocks, one per process, a local copy of which is kept
    in each process with the following rules for clock updates:

    * initially all clocks are zero
    * each time a process experiences an internal event, it increments
      its own logical clock in the vector by one
    * each time a process prepares to send a message, it increments its
      own logical clock in the vector by one and then sends its entire
      vector along with the message being sent
    * each time a process receives a message, it increments its own
      logical clock in the vector by one and updates each element in its
      vector by taking the maximum of the value in its own vector clock
      and the value in the vector in the received message (for every
      element).

You're encouraged to read the original paper, linked below.


=head1 METHODS

=head2 Algorithm::VectorClocks->new([$id or $vc])

Creates a new object of Algorithm::VectorClocks.
Arguments can be any one of the following:

=over 4

=item * Node ID

The name of a node which manages the vector clocks.

=item * Algorithm::VectorClocks object

An Algorithm::VectorClocks object.
In this case, this method returns the passed argument $vc.

=item * Serialized object of Algorithm::VectorClocks

A serialized form of Algorithm::VectorClocks object,
which can be obtained by method serialize().
In this case, this method behaves like a copy constructor.

=back

If no arguments are given, your hostname is used as a Node ID.


=head2 $vc->serialize

Returns a serialized form of $vc,
which is intended to be exchanged with the other nodes.

This module overloads a string conversion operator,
and the following code does the same thing:

    "$vc"

=head2 $vc->increment

Increments its own clock in $vc, and returns the object itself.

This module overloads an increment operator,
and the following code does the same thing:

    $vc++;

=head2 $vc->merge($other_vc)

Merges $other_vc into $vc itself, and returns the object itself.

This module overloads an assignment forms of additional operator,
and the following code does the same thing:

    $vc += $other_vc;

=head2 $vc->equal($other_vc)

Returns true if $vc equals $other_vc.

This module overloads a comparison operator,
and the following code does the same thing:

    $vc == $other_vc;
    $vc eq $other_vc;

=head2 $vc->not_equal($other_vc)

Returns true unless $vc equals $other_vc.

This module overloads a comparison operator,
and the following code does the same thing:

    $vc != $other_vc;
    $vc ne $other_vc;


=head2 $vc->id

Returns the node ID.


=head2 $vc->clocks

Returns the vector of clocks.


=head2 order_vector_clocks($vc, ...)

Returns the passed vector clocks $vc's in order (the latest comes first).
The arguments can be Algorithm::VectorClock objects or serialied ones,

If some vector clocks are independently updated and cannot be ordered,
they are packed into single element as an array reference.
In the following example, vector clocks of A and C are independent with each other,
and B is older than them:

    @vcs = order_vector_clocks(@vcs);

    $vcs[0][0]; # is Algorithm::VectorClocks object of 'A'
    $vcs[0][1]; # is Algorithm::VectorClocks object of 'C'
    $vcs[1];    # is Algorithm::VectorClocks object of 'B'


=head1 INTERNAL METHODS

=head2 _are_independent($vc, ...)

Returns true if any pair of $vc's are independent with each other.

=head2 $vc->_is_independent($other_vc)

Returns true if $vc and $other_vc are independent with each other.

=head2 $vc->_compare($other_vc)

Compares $vc with $other_vc,
which can be an Algorithm::VectorClock object or a serialied one.

Returns a negative value if $vc is a cause of $other_vc.
Returns a positive value if $vc is an effect of $other_vc.
Returns zero if $vc and $other_vc are same or independent.

=head2 _list_ids(@vcs)


=head1 SEE ALSO

Friedemann Mattern, "Virtual Time and Global States of Distributed Systems," Proc. Workshop on Parallel and Distributed Algorithms, 1989.


=head1 AUTHOR

Takeru INOUE  C<< <takeru.inoue _ gmail.com> >>


=head1 LICENCE AND COPYRIGHT

Copyright (c) 2008, Takeru INOUE C<< <takeru.inoue _ gmail.com> >>. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.


=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.
