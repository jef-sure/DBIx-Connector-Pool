use FindBin qw($Bin);
use lib "$Bin/../lib";
use lib "$Bin/../t";
use Test::More;
use DBI;
use Coro;
use AnyEvent;
use Coro::AnyEvent;
use Time::HiRes 'time';
use DBIx::Connector::Pool;
use DBIx::Connector;
use PgSet;

SKIP: {
	eval {require DBIx::Struct; DBIx::Struct->import('connector') };
	skip "no DBIx::Struct" if $@;
	ok(PgSet::initdb,  'local postgres db initialized');
	ok(PgSet::startdb, 'local postgres db started');
	skip "no test connect"
		unless DBIx::Connector->new("dbi:Pg:dbname=postgres", $PgSet::testuser, '', {RootClass => 'DBIx::PgCoroAnyEvent'});
	ok(
		my $pool = DBIx::Connector::Pool->new(
			dsn        => "dbi:Pg:dbname=postgres",
			user       => $PgSet::testuser,
			password   => '',
			initial    => 1,
			keep_alive => 1,
			max_size   => 5,
			tid_func   => sub {"$Coro::current" =~ /(0x[0-9a-f]+)/i; hex $1},
			wait_func => sub        {Coro::AnyEvent::sleep 0.05},
			attrs     => {RootClass => 'DBIx::PgCoroAnyEvent'}
		),
		'created pool'
	);
	DBIx::Struct::set_connector_pool($pool);
	ok(DBIx::Struct::connect("postgres", $PgSet::testuser, ''), 'DBIx::Struct connected');
	$pool->get_connector->run(
		sub {
			$_->do(
				q{
					create table products 
					as select 
                           generate_series(1,1000) as id,
                           md5(random()::text)::char(10) as name,
                           (random()*1000)::numeric(10,2) as price,
                           (random() * 21 + 22)::int as size,
                           (array['cyan','magenta'])[ceil(random()*2)] as color,
                           (now() - interval '1 day' * round(random()*100))::timestamp(0) as updated_at,
                           (now() - interval '2 year' + interval '1 year' * random())::date as built,
                           random()::int::bool as avail
				}
			);
			$_->do(q{alter table products add primary key (id)});
		}
	);
	my @async;
	for my $th (1 .. 10) {
		push @async, async {
			my $products = all_rows(
				products => -where => {id => {">", ($th - 1) * 100 , "<=", ($th - 1) * 100 + 100}},
				sub {$_->data}
			);
			is(scalar @$products, 100, "selected 100 products");
		};
	}

	for (@async) {
		$_->join;
	}

}
done_testing();

