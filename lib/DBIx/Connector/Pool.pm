package DBIx::Connector::Pool::Item::_on_destroy;

sub new {
	${$_[1]} //= 0;
	++${$_[1]};
	bless [@_[1 .. $#_]];
}

sub DESTROY {
	$_[0][1](@{$_[0]}[2 .. $#_]) if --${$_[0][0]} == 0;
}

package DBIx::Connector::Pool::Item;
use warnings;
use strict;

our @ISA              = ('DBIx::Connector');
our $not_in_use_event = sub {
	$_[0]->used_now;
};

sub _rebase {
	my ($base) = @_;
	if ($base) {
		$ISA[0] = $base;
	}
}

sub import {
	my ($class, $base) = @_;
	_rebase $base;
}

sub used_now {
	$_[0]->{_item_last_use} = time;
}

sub txn {
	my $use_guard
		= DBIx::Connector::Pool::Item::_on_destroy::new(\$_[0]->{_item_in_use}, $not_in_use_event, $_[0]);
	$_[0]->used_now;
	shift->SUPER::txn(@_);
}

sub run {
	my $use_guard
		= DBIx::Connector::Pool::Item::_on_destroy::new(\$_[0]->{_item_in_use}, $not_in_use_event, $_[0]);
	$_[0]->used_now;
	shift->SUPER::run(@_);
}

sub item_in_use {
	$_[0]->{_item_in_use};
}

sub item_last_use {
	$_[0]->{_item_last_use};
}

package DBIx::Connector::Pool;
use warnings;
use strict;
use Carp;
use DBI;

sub new {
	my ($class, %args) = @_;
	$args{initial}   //= 1;
	$args{tid_func}  //= sub {1};
	$args{wait_func} //= sub {croak "real waiting function must be supplied"};
	$args{max_size} ||= -1;
	$args{keep_alive} //= -1;
	$args{user}       //= ((getpwuid $>)[0]);
	$args{password}   //= '';
	$args{attrs}      //= {};
	$args{dsn}        //= 'dbi:Pg:dbname=' . $args{user};
	if ($args{max_size} > 0 && $args{initial} != 0 && $args{max_size} > $args{initial}) {
		$args{initial} = $args{max_size};
	}
	$args{pool} = [];
	my $self = bless \%args, $class;
	if ($args{connector_base}) {
		DBIx::Connector::Pool::Item::_rebase($args{connector_base});
	}
	$self->make_initial;
	$self;
}

sub make_initial {
	my $self = $_[0];
	for my $i (0 .. $self->{initial} - 1) {
		$self->{pool}[$i] = {
			tid       => undef,
			connector => (
				       DBIx::Connector::Pool::Item->new($self->{dsn}, $self->{user}, $self->{password}, $self->{attrs})
					or croak "Can't create initial connect: " . DBI::errstr
			)
		};
	}
}

sub in_use_size {
	my $self   = $_[0];
	my $in_use = 0;
	for my $i (0 .. @{$self->{pool}} - 1) {
		if (defined($self->{pool}[$i]{tid}) && $self->{pool}[$i]{connector}) {
			++$in_use;
		}
	}
}

sub collect_unused {
	my $self   = $_[0];
	my $in_use = $self->in_use_size;
	return if $in_use <= $self->{initial};
	my $i;
	my $remove_sub = sub {
		if ($i == @{$self->{pool}} - 1) {
			pop @{$self->{pool}};
		} else {
			$self->{pool}[$i] = {};
		}
		--$in_use;
	};
	for ($i = @{$self->{pool}} - 1; $i >= 0 && $in_use <= $self->{initial}; --$i) {
		if ($self->{pool}[$i]{connector} && !$self->{pool}[$i]{connector}->item_in_use) {
			if (time - $self->{pool}[$i]{connector}->item_last_use > $self->{keep_alive}) {
				$remove_sub->();
			} else {
				$self->{pool}[$i]{tid} = undef;
			}
		} elsif (!$self->{pool}[$i]{connector}) {
			$remove_sub->();
		}
	}
}

sub get_connector {
	my $self = $_[0];
	my $tid  = $self->{tid_func}();
	for my $i (0 .. @{$self->{pool}} - 1) {
		if (defined($self->{pool}[$i]{tid}) && $self->{pool}[$i]{tid} == $tid && $self->{pool}[$i]{connector}) {
			$self->{pool}[$i]{connector}->used_now;
			return $self->{pool}[$i]{connector};
		}
	}
RESELECT:
	do {
		for my $i (0 .. @{$self->{pool}} - 1) {
			if ($self->{pool}[$i]{connector}
				&& (!defined($self->{pool}[$i]{tid}) || !$self->{pool}[$i]{connector}->item_in_use))
			{
				$self->{pool}[$i]{tid} = $tid;
				$self->{pool}[$i]{connector}->used_now;
				return $self->{pool}[$i]{connector};
			}
		}
		$self->{wait_func}() if @{$self->{pool}} >= $self->{max_size};
	} while (@{$self->{pool}} >= $self->{max_size});
	my $connector = eval {DBIx::Connector::Pool::Item->new($self->{dsn}, $self->{user}, $self->{password}, $self->{attrs})};
	if (!$connector) {
		if ($self->{initial} > 0 && $self->{max_size} > $self->{initial}) {
			--$self->{max_size};
			carp "Corrected connector pool size: $self->{max_size}";
			goto RESELECT;
		} else {
			croak "Can't create new connector: " . DBI::errstr;
		}
	}
	$connector->used_now;
	push @{$self->{pool}}, {tid => $tid, connector => $connector};
	$connector;
}

1;
