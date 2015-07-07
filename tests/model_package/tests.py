# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from django import db

from django.contrib.sites.models import Site
from django.db import models, get_cached_connection, transaction, connection
from django.test import TestCase

from .models.publication import Publication
from .models.article import Article


class Advertisement(models.Model):
    customer = models.CharField(max_length=100)
    publications = models.ManyToManyField(
        "model_package.Publication", null=True, blank=True
    )


class ModelPackageTests(TestCase):

    def test_cached_connection(self):
        """
            python tests/runtests.py model_package.ModelPackageTests.test_cached_connection
            和默认的connection独立，因此需要单独处理
        """
        # with get_cached_connection() as connection:
        print "connection: ", connection
        p = Publication.objects.using(connection=connection).create(title="FooBar")

        current_site = Site.objects.get_current(connection)
        self.assertEqual(current_site.domain, "example.com")

        site1 = Site.objects.using(connection=connection).create(domain="xxxx", name="hello")

        print "Publication Count: ", Publication.objects.using(connection=connection).count()

        # Regression for #12168: models split into subpackages still get M2M
        # tables
        a = Article.objects.using(connection=connection).create(headline="a foo headline")
        a.publications.using(connection=connection).add(p)
        a.sites.using(connection=connection).add(current_site)
        a.sites.using(connection=connection).add(site1)

        print "SiteCount1: ", Site.objects.using(connection=connection).count()
        print "SiteCount2: ", Site.objects.count()

        a = Article.objects.using(connection=connection).get(id=a.pk)
        self.assertEqual(a.id, a.pk)
        self.assertEqual(a.sites.count(), 2)

        # Regression for #12245 - Models can exist in the test package, too
        ad = Advertisement.objects.using(connection=connection).create(customer="Lawrence Journal-World")
        ad.publications.using(connection=connection).add(p)

        ad = Advertisement.objects.using(connection=connection).get(id=ad.pk)

        print "publications: ", ad.publications
        self.assertEqual(ad.publications.count(), 1)

        # Regression for #12386 - field names on the autogenerated intermediate
        # class that are specified as dotted strings don't retain any path
        # component for the field or column name
        self.assertEqual(
            Article.publications.through._meta.fields[1].name, 'article'
        )
        self.assertEqual(
            Article.publications.through._meta.fields[1].get_attname_column(),
            ('article_id', 'article_id')
        )
        self.assertEqual(
            Article.publications.through._meta.fields[2].name, 'publication'
        )
        self.assertEqual(
            Article.publications.through._meta.fields[2].get_attname_column(),
            ('publication_id', 'publication_id')
        )

        # The oracle backend truncates the name to 'model_package_article_publ233f'.
        self.assertTrue(
            Article._meta.get_field('publications').m2m_db_table() in ('model_package_article_publications', 'model_package_article_publ233f')
        )

        self.assertEqual(
            Article._meta.get_field('publications').m2m_column_name(), 'article_id'
        )
        self.assertEqual(
            Article._meta.get_field('publications').m2m_reverse_name(),
            'publication_id'
        )

            # finally:
            #     transaction.rollback(connection=connection)

    def test_model_packages(self):
        p = Publication.objects.create(title="FooBar")

        current_site = Site.objects.get_current()
        self.assertEqual(current_site.domain, "example.com")

        # Regression for #12168: models split into subpackages still get M2M
        # tables
        a = Article.objects.create(headline="a foo headline")
        a.publications.add(p)
        a.sites.add(current_site)

        a = Article.objects.get(id=a.pk)
        self.assertEqual(a.id, a.pk)
        self.assertEqual(a.sites.count(), 1)

        # Regression for #12245 - Models can exist in the test package, too
        ad = Advertisement.objects.create(customer="Lawrence Journal-World")
        ad.publications.add(p)

        ad = Advertisement.objects.get(id=ad.pk)
        self.assertEqual(ad.publications.count(), 1)

        # Regression for #12386 - field names on the autogenerated intermediate
        # class that are specified as dotted strings don't retain any path
        # component for the field or column name
        self.assertEqual(
            Article.publications.through._meta.fields[1].name, 'article'
        )
        self.assertEqual(
            Article.publications.through._meta.fields[1].get_attname_column(),
            ('article_id', 'article_id')
        )
        self.assertEqual(
            Article.publications.through._meta.fields[2].name, 'publication'
        )
        self.assertEqual(
            Article.publications.through._meta.fields[2].get_attname_column(),
            ('publication_id', 'publication_id')
        )

        # The oracle backend truncates the name to 'model_package_article_publ233f'.
        self.assertTrue(
            Article._meta.get_field('publications').m2m_db_table() in ('model_package_article_publications', 'model_package_article_publ233f')
        )

        self.assertEqual(
            Article._meta.get_field('publications').m2m_column_name(), 'article_id'
        )
        self.assertEqual(
            Article._meta.get_field('publications').m2m_reverse_name(),
            'publication_id'
        )
