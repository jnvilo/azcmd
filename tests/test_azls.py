import unittest
from azcmd import azls


class MyTestCase(unittest.TestCase):

    def test_azget_storage_account_names(self):
        storage_accounts = azls.azls_storage_account_names()
        self.assertTrue(len(storage_accounts) > 0)
        print(storage_accounts)

    def test_azget_container_paths(self):
        containers = azls.azls_container_paths()
        #self.assertTrue(len(containers) > 0)
        print(containers)




if __name__ == '__main__':
    unittest.main()
