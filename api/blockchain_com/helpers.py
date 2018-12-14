#
"""Contains helpers for interacting with Blockchain.com API.

See documentation: https://www.blockchain.com/charts.
"""
import re


class BlockChainDotComEndpointBases:
    """
    """

    def __init__(self):
        # CURRENCY STATISTICS
        self.TOTAL_BITCOINS = "https://api.blockchain.info/charts/total-bitcoins"
        # BLOCK DETAILS
        self.BLOCKS_SIZE = "https://api.blockchain.info/charts/blocks-size"
        self.AVG_BLOCK_SIZE = "https://api.blockchain.info/charts/avg-block-size"
        self.N_TRANSACTIONS_PER_BLOCK = "https://api.blockchain.info/charts/n-transactions-per-block"
        self.MEDIAN_CONFIRMATION_TIME = "https://api.blockchain.info/charts/median-confirmation-time"
        # MINING INFORMATION
        self.HASH_RATE = "https://api.blockchain.info/charts/hash-rate"
        self.DIFFICULTY = "https://api.blockchain.info/charts/difficulty"
        self.MINERS_REVENUE = "https://api.blockchain.info/charts/miners-revenue"
        self.TRANSACTION_FEES = "https://api.blockchain.info/charts/transaction-fees"
        self.TRANSACTION_FEES_USD = "https://api.blockchain.info/charts/transaction-fees-usd"
        self.COST_PER_TRANSACTION_PERCENT = "https://api.blockchain.info/charts/cost-per-transaction-percent"
        self.COST_PER_TRANSACTION = "https://api.blockchain.info/charts/cost-per-transaction"
        # NETWORK ACTIVITY
        self.N_UNIQUE_ADDRESSES = "https://api.blockchain.info/charts/n-unique-addresses"
        self.N_TRANSACTIONS = "https://api.blockchain.info/charts/n-transactions"
        self.N_TRANSACTIONS_TOTAL = "https://api.blockchain.info/charts/n-transactions-total"
        self.TRANSACTIONS_PER_SECOND = "https://api.blockchain.info/charts/transactions-per-second"  # TODO: requires special handling
        self.MEMPOOL_COUNT = "https://api.blockchain.info/charts/mempool-count"  # TODO: requires special handling
        self.MEMPOOL_GROWTH = "https://api.blockchain.info/charts/mempool-growth"  # TODO: requires special handling
        self.MEMPOOL_SIZE = "https://api.blockchain.info/charts/mempool-size"  # TODO: requires special handling
        self.UTXO_COUNT = "https://api.blockchain.info/charts/utxo-count"
        self.N_TRANSACTIONS_EXCLUDING_POPULAR = "https://api.blockchain.info/charts/n-transactions-excluding-popular"
        self.N_TRANSACTIONS_EXCLUDING_CHAINS_LONGER_THAN_100 = "https://api.blockchain.info/charts/n-transactions-excluding-chains-longer-than-100"
        self.OUTPUT_VOLUME = "https://api.blockchain.info/charts/output-volume"
        self.ESTIMATED_TRANSACTION_VOLUME = "https://api.blockchain.info/charts/estimated-transaction-volume"
        self.ESTIMATED_TRANSACTION_VOLUME_USD = "https://api.blockchain.info/charts/estimated-transaction-volume-usd"
        # BLOCKCHAIN WALLET ACTIVITY
        self.MY_WALLET_N_USERS = "https://api.blockchain.info/charts/my-wallet-n-users"


def get_chart_names_url_bases():
    """Get chart names in URL endpoints for https://api.blockchain.info/charts stored in `BlockChainDotComEndpointBases`

    Returns: List containing chart names.

    """
    endpoint_dict = BlockChainDotComEndpointBases().__dict__
    endpoint_keys = endpoint_dict.keys()
    url_bases = endpoint_dict.values()
    chart_names = [re.sub('.*charts/', '', url_base) for url_base in url_bases]
    # confirm key names align with chart names
    endpoint_keys_frmt = [re.sub('_', '-', k.lower()) for k in endpoint_keys]
    assert endpoint_keys_frmt == chart_names, "Endpoint key names don't match chart names in URLs!"
    return chart_names, url_bases
