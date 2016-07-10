from __future__ import unicode_literals

import logging
from sorl.thumbnail.compat import string_type, text_type
from sorl.thumbnail.conf import settings, defaults as default_settings
from sorl.thumbnail.helpers import serialize, deserialize
from sorl.thumbnail.images import ImageFile, DummyImageFile
from sorl.thumbnail.kvstores.base import add_prefix
from sorl.thumbnail import default
from sorl.thumbnail.base import ThumbnailBackend

from sorlery.tasks import create_thumbnail

logger = logging.getLogger(__name__)


class QueuedThumbnailBackend(ThumbnailBackend):
    """
    Queue thumbnail generation with django-celery.
    """

    def get_thumbnail(self, file_, geometry_string, **options):
        """
        Returns thumbnail as an ImageFile instance for file with geometry and
        options given. First it will try to get it from the key value store,
        secondly it will create it.
        """
        logger.debug(text_type('Getting thumbnail for file [%s] at [%s]'),
                     file_, geometry_string)

        async = options.pop('async', True)
        if not async:
            return super(QueuedThumbnailBackend, self).get_thumbnail(
                file_, geometry_string, **options)

        if file_:
            source = ImageFile(file_)
        elif settings.THUMBNAIL_DUMMY:
            return DummyImageFile(geometry_string)
        else:
            return None

        # preserve image filetype
        if settings.THUMBNAIL_PRESERVE_FORMAT:
            options.setdefault('format', self._get_format(source))

        for key, value in self.default_options.items():
            options.setdefault(key, value)

        # For the future I think it is better to add options only if they
        # differ from the default settings as below. This will ensure the same
        # filenames being generated for new options at default.
        for key, attr in self.extra_options:
            value = getattr(settings, attr)
            if value != getattr(default_settings, attr):
                options.setdefault(key, value)

        name = self._get_thumbnail_filename(source, geometry_string, options)
        thumbnail = ImageFile(name, default.storage)
        cached = default.kvstore.get(thumbnail)
        if cached:
            return cached

        # We cannot check if the file exists, as remote storage is slow. If
        # we have reached this point, the image does not exist in our kvstore
        # so create the entry and queue the generation of the image.
        #
        # Note: If the thumbnail file has been deleted, you will need to manually
        # clear the corresponding row from the kvstore to have thumbnail rebuilt.
        job = create_thumbnail.delay(file_, geometry_string, options, name)
        if isinstance(file_, string_type):
            filename = file_.split('/')[-1]
        else:
            filename = file_.name
        if job:
            geometry = (0, 0)
            # We can't add a source row to the kvstore without the size
            # information being looked up, so add dummy information here
            # We'll need to correct this information when we generate the thumbnail
            source.set_size(geometry)
            default.kvstore.get_or_set(source)

            # We don't want to do any file access in this thread, so we tell sorlery
            # to proceed as normal and cheekily update the name and storage after
            # the hash has been calculated.
            thumbnail.set_size(geometry)
            default.kvstore.set(thumbnail, source)

            # Now we go back and manually update the thumbnail to point at the source image
            # Hopefully someone can suggest a better way to do this ... but the sorl internals
            # don't make it easy to.
            rawvalue = default.kvstore._get_raw(add_prefix(thumbnail.key))
            rawvaluearr = deserialize(rawvalue)
            rawvaluearr['name'] = filename
            default.kvstore._set_raw(add_prefix(thumbnail.key),
                                     serialize(rawvaluearr))

        thumbnail.name = filename
        return thumbnail
