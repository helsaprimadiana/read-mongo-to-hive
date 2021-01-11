def get_selectexpr_storage(db_cl_name):
    if db_cl_name.lower() == "jenius_transaction_requests_push-payments":
        column_selectexpr = ['_id.oid as _id', 'createdAt as createdAt', 'modifiedAt as modifiedAt', 'expiresAt as expiresAt', 'cif as cif', 'sender.id as sender_id', 'sender.avatarUrl as sender_avatarUrl', 'sender.fullName as sender_fullName', 'sender.accountNumber as sender_accountNumber', 'sender.cashtag as sender_cashtag', 'sourceAccountNumber as sourceAccountNumber', 'recipient as recipient', 'recipientName as recipientName', 'amount as amount', 'notes as notes', 'categoryId as categoryId', 'recipientAccount.accountNumber as recipientAccount_accountNumber', 'recipientAccount.bankCode as recipientAccount_bankCode', 'recipientAccount.bankName as recipientAccount_bankName', 'recipientAccount.ownerName as recipientAccount_ownerName', 'recipientAccount.jenius as recipientAccount_jenius', 'status as status', 'shortcode as shortcode', 'holdNumber as holdNumber', 'shortcodeId as shortcodeId', 'expiredAt as expiredAt']
    elif db_cl_name.lower() == "jenius_transaction_requests_transaction_requests":
        column_selectexpr = ['_id.oid as _id', 'targetAccountId as targetAccountId', 'type as type', 'totalAmount as totalAmount', 'deleted as deleted', 'senderId as senderId', 'createdAt as createdAt', 'modifiedAt as modifiedAt']
    elif db_cl_name.lower() == "jenius_transaction_requests_transaction_requests_requests":
        column_selectexpr = ['_id.oid as _id','requests._id as requests_id', 'requests.amount as requests_amount', 'requests.status as requests_status', 'requests.createdAt as requests_createdAt', 'requests.modifiedAt as requests_modifiedAt', 'requests.recipientPhone as requests_recipientPhone', 'requests.recipientName as requests_recipientName']
    return column_selectexpr


