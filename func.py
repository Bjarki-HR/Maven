def make_connection_String(driver, dbserver, db, username, password):
    """ Makes a connection string to database """
    return f"Driver={driver};Server={dbserver};Database={db};Uid={username};Pwd={password};Enrcypt=yes;TrustServerCertificate=no;Connection Timeout=30;"