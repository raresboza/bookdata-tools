import logging
from invoke import task

import support as s

_log = logging.getLogger(__name__)


@task(s.init)
def init(c, force=False):
    if s.start('loc-mds-init', force=force, fail=False):
        _log.info('initializing LOC schema')
        s.psql(c, 'loc-mds-schema.sql')
        s.finish('loc-mds-init')
    else:
        _log.info('LOC schema initialized')


@task(s.init)
def init_id(c, force=False):
    if s.start('loc-id-init', force=force, fail=False):
        _log.info('initializing LOC schema')
        s.psql(c, 'loc-id-schema.sql')
        s.finish('loc-id-init')
    else:
        _log.info('LOC schema initialized')


@task(s.build, s.init, init)
def import_books(c, force=False):
    "Import the LOC MDS data"
    s.start('loc-mds-books', force=force)
    loc = s.data_dir / 'LOC'
    files = list(loc.glob('BooksAll.2014.part*.xml.gz'))
    _log.info('importing LOC data from %d files', len(files))
    s.pipeline([
        [s.bdtool, 'parse-marc', '--db-schema', 'locmds', '-t', 'book_marc_field', '--truncate'] + files
    ])
    s.finish('loc-mds-books')

@task(s.build, s.init, init)
def import_names(c, force=False):
    "Import the LOC MDS name data"
    s.start('loc-mds-names', force=force)
    loc = s.data_dir / 'LOC'
    names = loc / 'Names.2014.combined.xml.gz'
    _log.info('importing LOC data from %s', loc)
    s.pipeline([
        [s.bin_dir / 'parse-marc', '--db-schema', 'locmds', '-t', 'name_marc_field', '--truncate', names]
    ])
    s.finish('loc-mds-names')


@task(s.init)
def index_books(c, force=False):
    "Index LOC MDS books data"
    s.check_prereq('loc-mds-books')
    s.start('loc-mds-book-index', force=force)
    _log.info('building LOC indexes')
    s.psql(c, 'loc-mds-index-books.sql', staged=True)
    s.finish('loc-mds-book-index')


@task(s.init)
def index_names(c, force=False):
    "Index LOC MDS name data"
    s.check_prereq('loc-mds-names')
    s.start('loc-mds-name-index', force=force)
    _log.info('building LOC name indexes')
    s.psql(c, 'loc-mds-index-names.sql', True)
    s.finish('loc-mds-name-index')


@task(s.build, s.init, init_id)
def import_id_auth(c, force=False, convert_only=False, convert=True):
    s.start('loc-id-names', force=force)
    loc = s.data_dir / 'LOC'
    auth = loc / 'authoritiesnames.nt.both.zip'
    if convert:
        _log.info('converting authority ntriples to PSQL')
        s.pipeline([
            [s.bdtool, 'import-ntriples', '--db-schema', 'locid', '--prefix', 'auth', '--truncate', auth]
        ])

    s.finish('loc-id-names')


@task(s.build, s.init, init_id)
def import_id_work(c, force=False, convert_only=False, convert=True):
    s.start('loc-id-works', force=force)
    loc = s.data_dir / 'LOC'
    auth = loc / 'bibframeworks.nt.zip'
    if convert:
        _log.info('converting BIBFRAME ntriples to PSQL')
        s.pipeline([
            [s.bdtool, 'import-ntriples', '--db-schema', 'locid', '--prefix', 'work', '--truncate', auth]
        ])

    s.finish('loc-id-works')


@task(s.build, s.init, init_id)
def import_id_instance(c, force=False, convert_only=False, convert=True):
    s.start('loc-id-instances', force=force)
    loc = s.data_dir / 'LOC'
    auth = loc / 'bibframeinstances.nt.zip'
    if convert:
        _log.info('converting BIBFRAME ntriples to PSQL')
        s.pipeline([
            [s.bdtool, 'import-ntriples', '--db-schema', 'locid', '--prefix', 'instance', '--truncate', auth]
        ])

    s.finish('loc-id-instances')


@task(s.init)
def index_id_triples(c, force=False):
    "Create basic indexes on LOC ID data"
    s.check_prereq('loc-id-init')
    s.check_prereq('loc-id-names')
    s.check_prereq('loc-id-instances')
    s.check_prereq('loc-id-works')
    s.check_prereq('loc-id-instances')
    s.start('loc-id-triple-index', force=force)
    _log.info('building LOC name indexes')
    s.psql(c, 'loc-id-triple-index.sql', staged=True)
    s.finish('loc-id-triple-index')


@task(s.init)
def index_id_books(c, force=False):
    "Create book indexes on LOC ID data"
    s.check_prereq('loc-id-init')
    s.check_prereq('loc-id-works')
    s.check_prereq('loc-id-instances')
    s.check_prereq('loc-id-triple-index')
    s.start('loc-id-book-index', force=force)
    _log.info('building LOC name indexes')
    s.psql(c, 'loc-id-book-index.sql', staged=True)
    s.finish('loc-id-book-index')


@task(s.init, s.build)
def record_mds_files(c):
    files = list((s.data_dir / 'LOC').glob('BooksAll.2014.*.xml.gz'))
    files.append(s.data_dir / 'LOC' / 'Names.2014.combined.xml.gz')
    s.booktool(c, 'hash', *files)


@task(s.init, s.build)
def record_id_files(c):
    fns = [
        'authoritiesnames.nt.both.zip',
        'bibframeinstances.nt.zip',
        'bibframeworks.nt.zip'
    ]
    files = [s.data_dir / 'LOC' / fn for fn in fns]
    s.booktool(c, 'hash', *files)
