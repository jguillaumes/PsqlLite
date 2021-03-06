//
//  PsqlLite.m
//  PsqlLite
//
//  Created by Jordi Guillaumes Pons on 27/3/16.
//  Copyright © 2016 Jordi Guillaumes Pons. All rights reserved.
//

#import "PsqlLite.h"
#include <stdlib.h>
#import <libpq-fe.h>

#define BYTEA_OID

// MARK: - PsqlConnection

NSString *PsqlErrorDomain = @"name.guillaumes.jordi.PsqlLite";
NSBundle *PsqlBundle = NULL;

//+
// Initialize: load bundle for internationalized strings
// Frameworks don't do it by themselves
//-
__attribute__((constructor))
static void PsqlLiteInitializer() {
    PsqlBundle = [NSBundle bundleForClass:[PsqlConnection class]];
#ifdef DEBUG
    NSLog(@"Bundle assigned to %@", PsqlBundle);
#endif
}

//+
// Pseudoprivate properties
//-
@interface PsqlConnection()
@property (readonly) PGconn *conn;
@end

@implementation PsqlConnection : NSObject

- (PsqlConnection *) init  {
    self = [super init];
    _conn = NULL;
    return self;
}

//-
// Establish a connection to the Postgresql server
//
// Use the URL notation:    postgres://server.name:port/database
//-
- (Boolean) connectWithUrl:(NSString*)url
                  userName:(NSString*)userName
                  password:(NSString*)password
                     error:(NSError**)error {
    NSString *theUrl = [ NSString stringWithFormat:@"%@?user=%@&password=%@", url, userName, password] ;
    _conn = PQconnectdb(theUrl.UTF8String);
    if (![self isConnected]) {
        if (error != NULL) {
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey : [self getErrorMessage] };
            *error = [[NSError alloc] initWithDomain:PsqlErrorDomain
                                                code: 1
                                            userInfo: userInfo];
        }
        return false;
    }
    return true;
}

//+
// Get the last error message
//-
- (NSString *) getErrorMessage {
    NSString *msg = [NSString stringWithFormat: @"%s", PQerrorMessage(_conn)];
    return msg;
}

//+
// Return true if connected
//-
- (Boolean) isConnected {
    return PQstatus(_conn) == CONNECTION_OK;
}

//+
// Begin transaction
// If this method is not invoked, postgres will autocommit
//-
- (Boolean) begin {
    PGresult *pres = PQexec(_conn, "begin");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

//+
// Commit the current transaction
// No action is taken to check if there is actually a transaction in progress
//-
- (Boolean) commit {
    PGresult *pres = PQexec(_conn, "commit");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

//+
// Rollback the current transaction
// No action is taken to check if there is actually a transaction in progress
//-
- (Boolean) rollback {
    PGresult *pres = PQexec(_conn, "rollback");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

//+
// Close the psql connection
//-
- (void) close {
    PQfinish(_conn);
}

//+
// Close the connection before destroying the instance
//-
- (void) destroy {
    [self close];
}

@end



// MARK: - PsqlResult

//+
// Pseudoprivate method
// We don't want to expose this method as public because it uses a libpq
// typedef, so we would have to "publish" all the libpq interface
//-
@interface PsqlResult()
-(PsqlResult*) initWithResult:(PGresult *) pres;
@end

@implementation PsqlResult {
    PGresult *theResult;
}

//+
// Initialize the object using a PGresult structure
// PGResult is the opaque typedef used by libpq to respresent the
// result of a command or query.
//-
-(PsqlResult *) initWithResult:(PGresult *) result {
    self = [super init];
    theResult = result;                     // Save the PGresult pointer
    _rowCount = PQntuples(theResult);       // Get the number of rows
    _columnCount = PQnfields(theResult);    // Get the number of columns
    _curRow = 0;                            // Set up at first row
    if (_rowCount > 0) {                    // Check if we have actual data
        _isEmpty = false;
    } else {
        _isEmpty = true;
    }
    return self;
}

//+
// Return a string value using the index in the returned column
// The column number is zero based. If the column number is out of range
// we throw an Obj-C exception, so we will rash the program.
// An invalid column is a programmer error, so we crash instead of returning
// an error or a swift-catchable exception, as per Apple guidelines.
//
// This method allows to specify the encoding in which psql returns the string
// data. For a mac client it _should_ be UTF-8.
//-
-(NSString *) getStringWithIndex:(int) colIndex encoding:(NSStringEncoding) encoding {
    NSString *theValue = NULL;
    
    if (colIndex < 0 || colIndex > _columnCount) {
        NSException *exc = [[NSException alloc] initWithName:@"colOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"colOutOfRange", nil, PsqlBundle, @"Columna fora d'abast")
                                                        userInfo:NULL ];
        [exc raise];
    }
    if (!self.isEOF) {                      // Check we are not past end of data
        if (PQgetisnull(theResult, _curRow, colIndex) != 1) {
            // Get the string value as NSString (libpq returns a C String)
            // The returned C string will be translated from the specified encoding
            theValue = [[NSString alloc] initWithCString:PQgetvalue(theResult, _curRow, colIndex)
                                                encoding:encoding];
        }
    }
    return theValue;
}


//+
// Return a string value using the index in the returned column
// The column number is zero based
//
// This method assumes the client encoding is UTF-8
//-
-(NSString *) getStringWithIndex:(int) colIndex {
    return [self getStringWithIndex:colIndex encoding:NSUTF8StringEncoding];
}

//+
// Return a string value using the column or result name specified in the query.
// If the passed column name does not exist, we throw an Obj-C exception so
// we crash the program. An invalid column is a programmer error, so we crash
// instead of returning an error or a swift-catchable exception, as per Apple guidelines.
//
// This method allows to specify the encoding in which psql returns the string
// data. For a mac client it _should_ be UTF-8.
//-
-(NSString *) getStringWithName:(NSString *) colName encoding:(NSStringEncoding) encoding {
    NSString *theValue = NULL;
    int colNum;
    
    if (!self.isEOF) {
        colNum = PQfnumber(theResult, colName.UTF8String);
        if (colNum != -1) {
            theValue = [self getStringWithIndex:colNum encoding: encoding];
        } else {
            NSString *errFmt = NSLocalizedStringFromTableInBundle(@"columnNotFound", nil, PsqlBundle, @"Columna no trobada");
            NSString *errMsg = [NSString stringWithFormat:errFmt, colName];
            NSException *exc = [[NSException alloc] initWithName:@"columnNotFound"
                                                          reason:errMsg
                                                        userInfo:NULL ];
            [exc raise];
        }
    }
    return theValue;
}

//+
// Return a string value using the column or result name specified in the query.
// If the passed column name does not exist, we throw an Obj-C exception so
// we crash the program. An invalid column is a programmer error, so we crash
// instead of returning an error or a swift-catchable exception, as per Apple guidelines.
//
// This method assumes the client encoding is UTF-8.
//-
-(NSString *) getStringWithName:(NSString *) colName {
    return [self getStringWithName:colName encoding: NSUTF8StringEncoding];
}


-(int) getIntWithIndex:(int) colIndex {
    int theInt = 0;
    NSString *valstr = [self getStringWithIndex:colIndex];
    if (valstr != NULL) {
        theInt = valstr.intValue;
    }
    return theInt;
}

-(int) getIntWithName:(NSString *) colName {
    int theInt = 0;
    NSString *valstr = [self getStringWithName:colName];
    if (valstr != NULL) {
        theInt = valstr.intValue;
    }
    return theInt;
}


-(NSNumber *) getNumberWithIndex:(int) colIndex {
    NSNumber *theNumber = NULL;
    NSString *valstr = [self getStringWithIndex:colIndex];
    if (valstr != NULL) {
        NSNumberFormatter *nf = [[NSNumberFormatter alloc] init];
        nf.numberStyle = NSNumberFormatterDecimalStyle;
        theNumber = [nf numberFromString:valstr];
    }
    return theNumber;
}

-(NSNumber *) getNumberWithName:(NSString *) colName {
    NSNumber *theNumber = NULL;
    NSString *valstr = [self getStringWithName:colName];
    if (valstr != NULL) {
        NSNumberFormatter *nf = [[NSNumberFormatter alloc] init];
        nf.numberStyle = NSNumberFormatterDecimalStyle;
        theNumber = [nf numberFromString:valstr];
    }
    return theNumber;
}

-(NSMutableData *) getBytesWithIndex:(int) colIndex {
    NSMutableData *theData = nil;
    unsigned char *buffer = NULL;
    unsigned char *binBuffer = NULL;
    // int len=0;
    size_t binLen=0;
    
    if (colIndex < 0 || colIndex > _columnCount) {
        NSException *exc = [[NSException alloc] initWithName:@"colOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"colOutOfRange", nil, PsqlBundle, @"Columna fora d'abast")
                                                    userInfo:NULL ];
        [exc raise];
    }
    if (!self.isEOF) {
        if (PQgetisnull(theResult, _curRow, colIndex) != 1) {
            // len    = PQgetlength(theResult, _curRow, colIndex);
            buffer = (unsigned char *) PQgetvalue(theResult, _curRow, colIndex);
            binBuffer = PQunescapeBytea(buffer, &binLen);
            theData = [[NSMutableData alloc] initWithBytes:binBuffer length:binLen];
            PQfreemem(binBuffer);
        }
    }
    return theData;
}

-(NSMutableData *) getBytesWithName:(NSString *) colName {
    NSMutableData *theData = nil;
    int colNum;
    
    if (!self.isEOF) {
        colNum = PQfnumber(theResult, colName.UTF8String);
        if (colNum != -1) {
            theData = [self getBytesWithIndex:colNum];
        } else {
            NSException *exc = [[NSException alloc] initWithName:@"columnNotFound"
                                                          reason:NSLocalizedStringFromTableInBundle(@"columnNotFound", nil, PsqlBundle, @"Columna no trobada")
                                                        userInfo:NULL ];
            [exc raise];
        }
    }
    return theData;
}

-(NSDate *) getDateTimeWithIndex:(int)colIndex format:(NSString *)format {
    NSDate *theDate = nil;
    NSString *valstr = [self getStringWithIndex:colIndex];
    if (valstr != NULL) {
        NSDateFormatter *df = [[NSDateFormatter alloc] init];
        df.dateFormat = format;
        [df setLenient:true];
        theDate = [df dateFromString:valstr];
    }
    return theDate;
}

-(NSDate *) getDateTimeWithName:(NSString *)colName format:(NSString *)format {
    NSDate *theDate = nil;
    int colNum = 0;
    
    if (!self.isEOF) {
        colNum = PQfnumber(theResult, colName.UTF8String);
        if (colNum != -1) {
            theDate = [self getDateTimeWithIndex:colNum format:format];
        } else {
            NSString *errFmt = NSLocalizedStringFromTableInBundle(@"columnNotFound", nil, PsqlBundle, @"Columna no trobada");
            NSString *errMsg = [NSString stringWithFormat:errFmt, colName];
            NSException *exc = [[NSException alloc] initWithName:@"columnNotFound"
                                                          reason:errMsg
                                                        userInfo:NULL ];
            [exc raise];
        }
    }
    return theDate;
}

-(NSDate *) getDateWithIndex:(int)colIndex {
    return [self getDateTimeWithIndex:colIndex format:@"yyyy-MM-dd"];
}

-(NSDate *) getDateWithName:(NSString *)colName {
    return [self getDateTimeWithName:colName format:@"yyyy-MM-dd"];
}

-(NSDate *) getDateTimeWithIndex:(int)colIndex {
    return [self getDateTimeWithIndex:colIndex format:@"yyyy-MM-dd HH:mm:ss" ];
}

-(NSDate *) getDateTimeWithName:(NSString *)colName {
    return [self getDateTimeWithName:colName format:@"yyyy-MM-dd HH:mm:ss"];
}


-(Boolean) isBOF {
    return (!_isEmpty) && (_curRow == 0);
}

-(Boolean) isEOF {
    return (_isEmpty) || (_curRow >= _rowCount);
}

-(Boolean) firstRow {
    if (! _isEmpty) {
        _curRow = 0;
        return true;
    } else {
        return false;
    }
}

-(Boolean) lastRow {
    if (! _isEmpty) {
        _curRow = _rowCount - 1;
        return true;
    } else {
        return false;
    }
}

-(Boolean) nextRow {
    if (!self.isEOF) {
        _curRow += 1;
        return true;
    } else {
        return false;
    }
}


-(void) close {
    PQclear(theResult);
    _rowCount = 0;
    _curRow = 0;
    _isEmpty = true;
}

@end

// MARK: - PsqlStatement

@implementation PsqlStatement {
    NSString  * _Nonnull theSqlString;
    NSString  * _Nullable stmtName;
    PsqlConnection __weak *theConn;
    int numParams;
    NSMutableArray *parametres;
    NSMutableArray *byteParams;
};

static int sequence=0;

- (PsqlStatement *) init {
    return [super init];
}

- (PsqlStatement*) initWithString:(NSString*) sqlString
                     pqConnection:(PsqlConnection*) pqConnection {
    
    self = [super init];
    theConn   = pqConnection;
    theSqlString = sqlString;
    _isOK = false;
    
    if (stmtName != nil) {
        [self unPrepare];
        stmtName = nil;
    }
    
    return self;
}

-(NSString *) getStatementName {
    return stmtName;
}

- (Boolean) prepare:(NSError **) error {
    int theSeq;
    _isOK = false;
    @synchronized (self) {
        theSeq = ++sequence;
    }
    stmtName  = [[NSString alloc] initWithFormat:@"STM%09d", theSeq];
#ifdef DEBUG
    NSLog(@"Created statement %@",stmtName);
#endif

    PGresult *pres = PQprepare(theConn.conn, stmtName.UTF8String,
                               theSqlString.UTF8String, 0, NULL);
    if (pres != NULL) {
        if (PQresultStatus(pres) == PGRES_COMMAND_OK) {
            pres = PQdescribePrepared(theConn.conn, stmtName.UTF8String);
            numParams = PQnparams(pres);
            parametres = [[NSMutableArray alloc] initWithCapacity:numParams];
            byteParams = [[NSMutableArray alloc] initWithCapacity:numParams];
            _isOK = true;
            if (error != NULL) *error = nil;
        }
    }
    if (! _isOK) {
        if (error != NULL) {
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey : [theConn getErrorMessage] };
            *error = [[NSError alloc] initWithDomain:PsqlErrorDomain
                                                code: 2
                                            userInfo: userInfo];
        }
    }
    return _isOK;
}

- (Boolean) setStringParmWithIndex:(int)index value:(NSString *) value {
    if (index < numParams && index >= 0) {
        parametres[index] = value;
        byteParams[index] = [NSNull null];
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"paramOutOfRange", nil, PsqlBundle, @"Parametre fora d'abast")
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}


- (Boolean) setIntParmWithIndex:(int)index value:(int) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%d", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        byteParams[index] = [NSNull null];
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"paramOutOfRange", nil, PsqlBundle, @"Parametre fora d'abast")
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}

- (Boolean) setLongParmWithIndex:(int)index value:(long) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%ld", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        byteParams[index] = [NSNull null];
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"paramOutOfRange", nil, PsqlBundle, @"Parametre fora d'abast")
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}


- (Boolean) setDoubleParmWithIndex:(int)index value:(double) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%f", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        byteParams[index] = [NSNull null];
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"paramOutOfRange", nil, PsqlBundle, @"Parametre fora d'abast")
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}


- (Boolean) setDateParmWithIndex:(int)index value:(nullable NSDate *)value {
    if (index < numParams && index >= 0) {
        NSDateFormatter *df = [[NSDateFormatter alloc] init];
        df.dateFormat = @"yyyy-MM-dd";
        NSString *dateParm = [[NSString alloc] initWithString:[df stringFromDate:value]];
        parametres[index] = dateParm;
        byteParams[index] = [NSNull null];
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"paramOutOfRange", nil, PsqlBundle, @"Parametre fora d'abast")
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}

- (Boolean) setDateTimeParmWithIndex:(int)index value:(nullable NSDate *)value {
    if (index < numParams && index >= 0) {
        NSDateFormatter *df = [[NSDateFormatter alloc] init];
        df.dateFormat = @"yyyy-MM-dd HH:mm:ss";
        NSString *dateParm = [[NSString alloc] initWithString:[df stringFromDate:value]];
        parametres[index] = dateParm;
        byteParams[index] = [NSNull null];
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"paramOutOfRange", nil, PsqlBundle, @"Parametre fora d'abast")
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}


- (Boolean) setByteaParmWithIndex:(int)index value:(NSData *)value {
    if (index < numParams && index >= 0) {
        parametres[index] = [NSNull null];
        byteParams[index] = value;
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:NSLocalizedStringFromTableInBundle(@"paramOutOfRange", nil, PsqlBundle, @"Parametre fora d'abast")
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}

- (int) execute:(NSError**) error {
    PGresult *res = NULL;
    int ps = 0;
    int nrecs = -1;
    int i=0;
    
    if (_isOK) {
        char **paramValues = calloc(sizeof(char*), numParams);
        int *paramLengths  = calloc(sizeof(int), numParams);
        int *paramFormats  = calloc(sizeof(int), numParams);
        for (i=0; i<numParams; i++) {
            if (byteParams[i] == [NSNull null]) {
                paramFormats[i] = 0;        // String data
                paramLengths[i] = 0;
                paramValues[i]  = (char *) [parametres[i] UTF8String];
            } else {
                NSData *theData = byteParams[i];
                paramFormats[i] = 1;        // Binary data
                paramLengths[i] = (int)    theData.length;
                paramValues[i]  = (char *) theData.bytes;
            }
        }
        res = PQexecPrepared(theConn.conn, stmtName.UTF8String, numParams, paramValues, paramLengths, paramFormats, 0);
        free(paramValues);
        free(paramLengths);
        free(paramFormats);
        ps = PQresultStatus(res);
        if (ps == PGRES_COMMAND_OK ||
            ps == PGRES_TUPLES_OK  ||
            ps == PGRES_SINGLE_TUPLE) {
            nrecs = atoi(PQcmdTuples(res));
            if (error != NULL) error = nil;
        } else {
            _isOK = false;
            if (error != NULL) {
                NSDictionary *userInfo = @{NSLocalizedDescriptionKey : [theConn getErrorMessage] };
                *error = [[NSError alloc] initWithDomain:PsqlErrorDomain
                                                    code: 3
                                                userInfo: userInfo];
            }
        }
    }
    return nrecs;
}

- (PsqlResult *) executeQuery:(NSError **) error {
    PsqlResult *pr = NULL;
    PGresult *res = NULL;
    int ps = 0;
    int i=0;
    
    if (_isOK) {
        char **paramValues = calloc(sizeof(char*), numParams);
        int *paramLengths = calloc(sizeof(int), numParams);
        int *paramFormats = calloc(sizeof(int), numParams);
        for (i=0; i<numParams; i++) {
            paramFormats[i] = 0;
            paramLengths[i] = 0;
            paramValues[i]  = (char *) [parametres[i] UTF8String];
        }
        res = PQexecPrepared(theConn.conn, stmtName.UTF8String, numParams, paramValues, paramLengths, paramFormats, 0);
        free(paramValues);
        free(paramLengths);
        free(paramFormats);
        ps = PQresultStatus(res);
        if (ps == PGRES_TUPLES_OK ||
            ps == PGRES_SINGLE_TUPLE) {
            pr = [[PsqlResult alloc] initWithResult:res];
            if (error != NULL) error = nil;
        } else {
            _isOK = false;
            if (error != NULL) {
                NSDictionary *userInfo = @{NSLocalizedDescriptionKey : [theConn getErrorMessage] };
                *error = [[NSError alloc] initWithDomain:PsqlErrorDomain
                                                    code: 3
                                                userInfo: userInfo];
            }
        }
    }
    return pr;
}

- (void) unPrepare {
    NSString *deallocCmd = [ NSString stringWithFormat:@"DEALLOCATE %@", stmtName ];
#ifdef DEBUG
    NSLog(@"Deallocating prepared statement %@",stmtName);
#endif
    PQexec(theConn.conn, [deallocCmd cStringUsingEncoding:NSUTF8StringEncoding]);
    _isOK = false;
    stmtName = NULL;
}

- (void) close {
    parametres = nil;
    byteParams = nil;
    [self unPrepare];
}

- (void) dealloc {
    [self unPrepare];
}

@end
