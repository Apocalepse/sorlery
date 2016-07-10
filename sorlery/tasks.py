from __future__ import unicode_literals

import logging

from celery import shared_task
from sorl.thumbnail.compat import text_type
from sorl.thumbnail.conf import settings
from sorl.thumbnail.images import ImageFile
from sorl.thumbnail import default

logger = logging.getLogger(__name__)


@shared_task
def create_thumbnail(file_, geometry_string, options, name, force=False):
    if file_:
        source = ImageFile(file_)
    else:
        return

    thumbnail = ImageFile(name, default.storage)

    # We have to check exists() because the Storage backend does not
    # overwrite in some implementations.
    if settings.THUMBNAIL_FORCE_OVERWRITE or not thumbnail.exists() or force:
        try:
            source_image = default.engine.get_image(source)
        except IOError as e:
            logger.exception(e)
            # if S3Storage says file doesn't exist remotely, don't try to
            # create it and exit early.
            # Will return working empty image type; 404'd image
            logger.warn(
                text_type('Remote file [%s] at [%s] does not exist'),
                file_, geometry_string)
            return

        # We might as well set the size since we have the image in memory
        try:
            image_info = default.engine.get_image_info(source_image)
            options['image_info'] = image_info
        except AttributeError:
            options['image_info'] = {}
        size = default.engine.get_image_size(source_image)
        source.set_size(size)

        try:
            default.backend._create_thumbnail(
                source_image, geometry_string, options, thumbnail)
            default.backend._create_alternative_resolutions(
                source_image, geometry_string, options, thumbnail.name)
        finally:
            default.engine.cleanup(source_image)

    # If the thumbnail exists we don't create it, the other option is
    # to delete and write but this could lead to race conditions so I
    # will just leave that out for now.
    default.kvstore.get_or_set(source)
    default.kvstore.set(thumbnail, source)
