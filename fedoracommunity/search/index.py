"""
Creates our search index and its field structure,
and then populates it with packages from yum repositories
"""

import copy
import os
import json
import gzip
import logging
import shutil
import tarfile
import threading
import traceback
import re
import pprint

import requests
import xapian
import pdc_client

from os.path import join

from fedoracommunity.search.utils import filter_search_string

import gi

gi.require_version('AppStreamGlib', '1.0')

from gi.repository import AppStreamGlib

# It is on the roof.
import fedoracommunity.pool

# how many time to retry a downed server
MAX_RETRY = 10

local = threading.local()
local.http = requests.session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=MAX_RETRY)
local.http.mount('https://', adapter)
local.http.mount('http://', adapter)
log = logging.getLogger()


def download_file(url, dest):
    dirname = os.path.dirname(dest)
    if not os.path.isdir(dirname):
        log.info("Creating directory %s" % dirname)
        os.makedirs(dirname)

    log.info("Downloading %s to %s" % (url, dest))
    r = local.http.get(url, stream=True)
    if not bool(r):
        log.warn("Can't download %s" % (url))
        return
    with open(dest, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if not chunk:
                continue
            f.write(chunk)

    # Extract the big one.
    if dest.endswith('.tar.gz'):
        log.info("Extracting %s in %s" % (dest, dirname))
        f = tarfile.open(dest)
        try:
            f.extractall(path=dirname)
        finally:
            f.close()

    return dest


class Indexer(object):
    def __init__(self, cache_path,
                 bodhi_url=None,
                 mdapi_url=None,
                 icons_url=None,
                 pdc_url=None,
                 pagure_url=None):

        self.cache_path = cache_path
        self.dbpath = join(cache_path, 'search')
        self.icons_path = join(cache_path, 'icons')
        self.default_icon = 'package_128x128.png'
        self.bodhi_url = bodhi_url or "https://bodhi.fedoraproject.org"
        self.mdapi_url = mdapi_url or "https://mdapi.fedoraproject.org"
        self.icons_url = icons_url or "https://dl.fedoraproject.org/pub/alt/screenshots"
        self.pagure_url = pagure_url or "https://src.fedoraproject.org/api/0"
        self.icon_cache = {}
        pdc_url = pdc_url or "https://pdc.fedoraproject.org/rest_api/v1"
        self.pdc = pdc_client.PDCClientWithPage(pdc_url, develop=True)
        adapter = requests.adapters.HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=MAX_RETRY)
        self.pdc.session.mount('https://', adapter)
        self.pdc.session.mount('http://', adapter)

        self.active_fedora_releases = self._get_active_fedora_releases()

        self.create_index()

    def _call_api(self, url):
        retry = 0
        data = {}
        while retry < 3:
            log.debug("API call (_call_api) try %d, url %s" % (retry, url))
            try:
                response = local.http.get(url)
                if bool(response):
                    data = response.json()
                break
            except:
                log.warning("API call (_call_api) failed for %s" % url)
            retry = retry + 1

        return data

    def create_index(self):
        """ Create a new index, and set up its field structure """
        log.warning("start create_index");
        self.db = xapian.WritableDatabase(self.dbpath, xapian.DB_CREATE_OR_OPEN)
        self.indexer = xapian.TermGenerator()
        self.indexer.set_stemmer(xapian.Stem("en"))
        log.warning("end create_index");

    def _get_active_fedora_releases(self):
        response = self._call_api(self.bodhi_url + "/releases/?page=1&row_per_page=20")
        releases = response.get('releases', [])
        pages = response.get('pages', [])
        for page in range(2, pages + 1):
            response = self._call_api(self.bodhi_url + ("/releases/?page=%d&row_per_page=20" % (page)))
            releases += response.get('releases', [])
        active_fedora_releases = []
        for release in releases:
            log.debug(release['dist_tag'])
            if release['id_prefix'].lower() == 'fedora' and\
               release['state'] in ['current', 'pending'] and\
               release['composed_by_bodhi']:
                log.debug(pprint.pformat(release))
                active_fedora_releases.append(int(release['version']))

        active_fedora_releases.sort(reverse=True)
        log.info("active fedora releases: " + " ".join(str(e) for e in active_fedora_releases))

        return active_fedora_releases

    def pull_icons(self):
        for release in reversed(self.active_fedora_releases):
            prefix = 'f%i' % release
            files = ['fedora-%i.xml.gz', 'fedora-%i-icons.tar.gz']
            for fname in files:
                fname = fname % release
                url = join(self.icons_url, prefix, fname)
                target = join(self.icons_path, 'tmp', str(release), fname)

                try:
                    stats = os.stat(target)
                except OSError:
                    # Presumably no such file locally.  get it.
                    download_file(url, target)
                    continue

                # Check the file to see if it is different
                response = local.http.head(url, allow_redirects=True)
                if not bool(response):
                    log.warn("Can't download %s, ignoring..." % (url))
                    continue
                remote_size = int(response.headers['content-length'])
                local_size = stats.st_size
                if remote_size == local_size:
                    log.debug("%r seems unchanged." % url)
                    continue

                # Otherwise, they differ.  So download.
                download_file(url, target)

    def cache_icons(self):
        for release in self.active_fedora_releases:
            fname = 'fedora-%i.xml.gz' % release
            target = join(self.icons_path, 'tmp', str(release), fname)

            metadata = AppStreamGlib.Store()

            with gzip.open(target, 'rb') as f:
                metadata.from_xml(f.read().decode("utf-8"), '')

            for app in metadata.get_apps():
                # Other types are 'stock' and 'unknown'
                icons = app.get_icons()
                pkgname = app.get_pkgnames()[0]

                # Pick the biggest one..
                icon = None
                for candidate in icons:
                    if not icon:
                        if candidate.get_kind().value_nick == 'cached':
                            icon = candidate
                        continue
                    if int(icon.get_width()) < int(candidate.get_width()):
                        icon = candidate

                if not icon:
                    continue

                # Move the file out of the temp dir and into place
                s = join(self.icons_path, 'tmp', str(release),
                         '{width}x{height}', '{value}')
                d = join(self.icons_path, '{value}')
                source = s.format(width=icon.get_width(), height=icon.get_height(),
                                  value=icon.get_name())
                destination = d.format(value=icon.get_name())

                try:
                    shutil.copy(source, destination)

                    # And hang the name in the dict for other code to find it
                    # ... but only if we succeeded at placing the icon file.
                    self.icon_cache[pkgname] = icon.get_name()
                except IOError as e:
                    log.warning("appstream metadata lied: %s %r" % (source, e))
                except OSError as e:
                    log.warning("fail %r->%r.  %r" % (source, destination, e))

        shutil.rmtree(join(self.icons_path, 'tmp'))

    def gather_pdc_packages(self, pkg_name=None):

        kwargs = dict()
        if pkg_name is not None:
            kwargs['name'] = pkg_name

        for component in self.pdc.get_paged(self.pdc['global-components']._, **kwargs):
            log.debug(component)
            yield component

    def latest_active(self, name, ignore=None):

        # First, check to see if it is retired.
        # PDCClient pulls connection information from /etc/pdc.d/
        # develop=True means: don't authenticate.
        ignore = ignore or []
        kwargs = dict(global_component=name, active=True, type='rpm')
        latest_version = None
        branch_info = None
        for branch in self.pdc.get_paged(self.pdc['component-branches']._, **kwargs):
            if re.match(r'f\d+', branch['name']):
                version = int(branch['name'].strip('f'))
                if version in ignore:
                    continue
                if latest_version:
                    latest_version = max(latest_version, version)
                    if latest_version == version:
                        branch_info = branch
                else:
                    latest_version = version
                    branch_info = branch

        if latest_version is None:
            # Check if we are a subpackage,
            # if so run latest_active with the main package name
            kwargs = {'name': name}
            for comp in self.pdc.get_paged(self.pdc['rpms']._, **kwargs):
                if name != comp.get('srpm_name'):
                    branch_info = self.latest_active(comp.get('srpm_name'))
                    return branch_info
            raise ValueError('There is no active branch tied to a Fedora release for package %s' % (name))
        return branch_info

    def construct_package_dictionary(self, package):
        """ Return structured package dictionary from a pkg package.

        Result looks like this::

           {base_package_name: {'name': base_package_name,
                                'summary': base_package_summary,
                                'description': base_package_summary,
                                'devel_owner': owner,
                                'icon': icon_name,
                                'package': None,
                                'upstream_url': url,
                                'sub_pkgs': [{'name': sub_package_name,
                                              'summary': sub_package_summary,
                                              'description': sub_package_description,
                                              'icon': icon_name,
                                              'package': package},
                                             ...]},
        """
        log.info("construct_package_dictionary start")
        package = copy.deepcopy(package)

        try:
            name = package['name']
            info = self.latest_active(name)
        except Exception as e:
            log.warning(traceback.format_exc())
            log.error(e)
            log.warning("Failed to get pdc info for %r" % name)
            return

        # Getting summary and description

        mdapi_pkg_url = '/'.join([self.mdapi_url, info['name'],
                                 'srcpkg', info['global_component']])

        meta_data = self._call_api(mdapi_pkg_url)
        package['summary'] = meta_data.get('summary', 'no summary in mdapi')
        package['description'] = meta_data.get('description', 'no description in mdapi')
        package['upstream_url'] = meta_data.get('url', 'no upstream url in mdapi')

        # Getting the owner name
        pagure_pkg_url = '/'.join([self.pagure_url, 'rpms', info['global_component']])
        owner_data = self._call_api(pagure_pkg_url)
        if owner_data.get('access_users') is not None:
            package['devel_owner'] = owner_data['access_users']['owner'][0]
        else:
            package['devel_owner'] = 'no owner info in src.fp.o'
        package['status'] = info['active']
        package['icon'] = self.icon_cache.get(name, self.default_icon)
        package['branch'] = info['name']
        sb = self.get_sub_packages(package)
        log.debug("types...")
        log.debug(type(sb))
        package['sub_pkgs'] = list(sb)
        for idx in range(len(package['sub_pkgs'])):
            branch = package['sub_pkgs'][idx]['branch']
            name = package['sub_pkgs'][idx]['name']

            if branch == 'master':
                branch = 'rawhide'

            url = "/".join([self.mdapi_url, branch, "files", name])
            package['sub_pkgs'][idx]['file_data'] = self._call_api(url)

        log.debug(type(package['sub_pkgs']))

        # This is a "parent" reference.  the base packages always have "none"
        # for it, but the sub packages have the name of their parent package in
        # it.
        package['package'] = None

        branch = package['branch']

        if branch == 'master':
            branch = 'rawhide'

        url = "/".join([self.mdapi_url, branch, "files", name])
        package['file_data'] = self._call_api(url)

        log.info("construct_package_dictionary end")
        return package

    def get_sub_packages(self, package):
        name = package['name']
        branch = package['branch']
        icon = package['icon']

        if branch == 'master':
            branch = 'rawhide'

        url = "/".join([self.mdapi_url, branch, "srcpkg", name])
        # response = local.http.get(url)

        retry = 0
        while retry < 3:
            try:
                response = local.http.get(url)
                break
            except:
                log.warning("API call (get_sub_packages) failed for %s" % url)
            retry = retry + 1
            log.warning("API call (get_sub_packages) retrying %d" % retry)

        if not bool(response):
            # TODO -- don't always do this.
            # if we get a 404, that's usually because the package is retired in
            # rawhide... but that's okay.  we just queried pkgdb, so we should
            # see if it is active in any other branches, and if it is, get the
            # sub-packages from there.
            # raise StopIteration
            return

        data = response.json()
        sub_package_names = sorted(set([
            p for p in data['co-packages'] if p != name
        ]))

        for sub_package_name in sub_package_names:
            url = "/".join([self.mdapi_url, branch, "pkg", sub_package_name])
            data = self._call_api(url)
            yield {
                'name': sub_package_name,
                'summary': data.get('summary', 'no summary in mdapi'),
                'description': data.get('description', 'no description in mdapi'),
                'icon': icon,
                'package': name,
                'branch': branch,
            }

    def index_files_of_interest(self, doc, package_dict):
        log.info("index_files_of_interest start")
        name = package_dict['name']
        branch = package_dict['branch']

        if branch == 'master':
            branch = 'rawhide'

        #url = "/".join([self.mdapi_url, branch, "files", name])
        #data = self._call_api(url)
        data = package_dict['file_data']
        if data.get('files') is not None:
            for entry in data['files']:
                filenames = entry['filenames'].split('/')
                for filename in filenames:
                    if filename.startswith('/usr/bin'):
                        # index executables
                        log.info("indexing exe file %s" % os.path.basename(filename))
                        exe_name = filter_search_string(os.path.basename(filename))
                        self.indexer.index_text_without_positions("EX__%s__EX" % exe_name)
        else:
            log.warn("Failed to get file list for %r" % name)
            return
        log.info("index_files_of_interest end")

    def index_packages(self):
        # This is a generator that yields dicts of package info that we index
        log.info("index_packages start")
        packages = self.gather_pdc_packages()
        log.info("got pdc high-level packages done")

        def io_work_init():
            log.info("create thread-local requests session...")
            local.http = requests.session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3)
            adapter.init_poolmanager(10, 10, False, timeout=60)
            local.http.mount('https://', adapter)
            local.http.mount('http://', adapter)

        def io_work(package):
            # Do all of the gathering...
            package = self.construct_package_dictionary(package)

            # If the package is retired in all branches, it is None here..
            if package is None:
                return None

            return package

        pool = fedoracommunity.pool.ThreadPool(20)
        packages_data = pool.map(io_work, io_work_init, packages)

        for package in packages_data:
            if package is None:
                continue
            # And then prepare everything for xapian
            log.info("Processing final details for %s" % package['name'])
            try:
                self._create_document(package)
            except Exception as e:
                log.warning(traceback.format_exc())
                log.error(e)
        self.db.close()
        log.info("index_packages end")

    def _create_document(self, package, old_doc=None):
        log.info("_create_document start")
        doc = xapian.Document()
        self.indexer.set_document(doc)
        filtered_name = filter_search_string(package['name'])
        filtered_summary = filter_search_string(package['summary'])
        filtered_description = filter_search_string(package['description'])
        filtered_owner = filter_search_string(package['devel_owner'])

        self.indexer.index_text_without_positions('EX__' + filtered_name + '__EX', 10, '')
        self.indexer.index_text_without_positions('EX__' + filtered_owner + '__EX', 10, '')

        name_parts = filtered_name.split('_')
        for i in range(20):
            if len(name_parts) > 1:
                for part in name_parts:
                    self.indexer.index_text_without_positions(part)
            self.indexer.index_text_without_positions(filtered_name, 10, '')

        for i in range(4):
            self.indexer.index_text_without_positions(filtered_summary)
        self.indexer.index_text_without_positions(filtered_description)

        self.index_files_of_interest(doc, package)

        for sub_package in package['sub_pkgs']:
            filtered_sub_package_name = filter_search_string(sub_package['name'])
            log.info("       indexing subpackage %s" % sub_package['name'])

            self.indexer.index_text_without_positions(filtered_sub_package_name)
            self.indexer.index_text_without_positions('EX__' + filtered_sub_package_name
                                                      + '__EX', 10, '')

            self.index_files_of_interest(doc, sub_package)

            # Set special sub-package icon if appstream has one
            sub_package['icon'] = self.icon_cache.get(
                sub_package['name'], self.default_icon)

            # If the parent has a dull icon, give it ours!
            if sub_package['icon'] != self.default_icon \
                    and package['icon'] == self.default_icon:
                package['icon'] = sub_package['icon']

            # remove anything we don't want to store
            del sub_package['package']

        # @@: Right now we're only indexing the first part of the
        # provides/requires, and not boolean comparison or version
        # for requires in package.requires:
        #    print requires[0]
        #    doc.fields.append(xappy.Field('requires', requires[0]))
        # for provides in package.provides:
        #    doc.fields.append(xappy.Field('provides', provides[0]))

        # remove anything we don't want to store and then store data in
        # json format
        del package['package']

        doc.set_data(json.dumps(package))

        # It seems that xapian db.replace_document still creates a new
        # document. In order to avoid duplicating the document we are
        # using add_document and then delete the old document.
        self.db.add_document(doc)
        if old_doc is not None:
            self.db.delete_document(old_doc.get_docid())
        self.db.commit()
        log.info("_create_document end")


def run(cache_path, bodhi_url=None,
        mdapi_url=None, icons_url=None, pdc_url=None, pagure_url=None):
    indexer = Indexer(cache_path, bodhi_url, mdapi_url,
                      icons_url, pdc_url, pagure_url)

    indexer.pull_icons()
    indexer.cache_icons()

    log.info("Indexing packages. run start")
    indexer.index_packages()
    log.info("Indexed a ton of packages. run end")
