import unittest
import azcmd.funcs as azfunc



class SubscriptionsTestCase(unittest.TestCase):

    def test_get_subscriptions(self):
        subscriptions = azfunc.get_subscriptions()
        self.assertTrue(len(subscriptions) > 0)
        print(subscriptions)


class StorageAccountsTestCase(unittest.TestCase):

    def test_get_storage_accounts(self):
        storage_accounts = azfunc.get_storage_accounts()
        self.assertTrue(len(storage_accounts) > 0)
        print(storage_accounts)


