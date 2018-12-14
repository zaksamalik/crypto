import re

from api.blockchain_com.helpers import BlockChainDotComEndpointBases
from helpers.aws import create_s3_folders


def get_charts_endpoint_names():
    """Get chart names in URL endpoints for https://api.blockchain.info/charts stored in `BlockChainDotComEndpointBases`

    Returns: List containing chart names.

    """
    endpoint_dict = BlockChainDotComEndpointBases().__dict__
    endpoint_keys = endpoint_dict.keys()
    chart_names = [re.sub('.*charts/', '', url_base) for url_base in endpoint_dict.values()]
    # confirm key names align with chart names
    endpoint_keys_frmt = [re.sub('_', '-', k.lower()) for k in endpoint_keys]
    assert endpoint_keys_frmt == chart_names, "Endpoint key names don't match chart names!"
    return chart_names


def main():
    """Create S3 buckets folders that match chart names.

    Returns:

    """

    chart_names = get_charts_endpoint_names()

    char_folder_names = ['/api/blockchain.com/' + cn for cn in chart_names]

    create_s3_folders(bucket_name='data.crypto', s3_folder_names=char_folder_names)


if __name__ == '__main__':
    main()
