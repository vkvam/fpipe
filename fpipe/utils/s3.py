def list_objects(client, bucket: str, prefix: str = None, use_generator: bool = True):
    """
    Get all objects as a generator.
    :param bucket: Bucket name
    :param prefix: Object prefix
    :param use_generator: Use generator
    :return: generator or list
    """

    args = {
        'Bucket': bucket
    }
    if prefix is not None:
        args['Prefix'] = prefix

    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(**args)

    def get_generator():
        for page in page_iterator:
            for item in page['Contents']:
                yield item
    return get_generator() if use_generator else list(get_generator())
