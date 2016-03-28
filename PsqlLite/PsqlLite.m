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

/*
 static PQprintOpt printopt = {
 1, 1, 0, 0, 0, 0, " ", "", "", NULL
 };
 */

// MARK: - PsqlConnection

static NSString *errorDomain = @"name.guillaumes.jordi.PsqlLite";


@interface PsqlConnection()
@property (readonly) PGconn *conn;
@end

@implementation PsqlConnection : NSObject



- (PsqlConnection *) init {
    self = [super init];
    _conn = NULL;
    return self;
}

- (Boolean) connectWithUrl:(NSString*)url
                  userName:(NSString*)userName
                  password:(NSString*)password
                     error:(NSError**)error {
    NSString *theUrl = [ NSString stringWithFormat:@"%@?user=%@&password=%@", url, userName, password] ;
    _conn = PQconnectdb([theUrl UTF8String]);
    if (![self isConnected]) {
        if (error != NULL) {
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey : [self getErrorMessage] };
            *error = [[NSError alloc] initWithDomain:errorDomain
                                               code: 1
                                           userInfo: userInfo];
        }
        return false;
    }
    return true;
}

- (NSString *) getErrorMessage {
    NSString *msg = [NSString stringWithFormat: @"%s", PQerrorMessage(_conn)];
    return msg;
}

- (Boolean) isConnected {
    return PQstatus(_conn) == CONNECTION_OK;
}

- (Boolean) begin {
    PGresult *pres = PQexec(_conn, "begin");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

- (Boolean) commit {
    PGresult *pres = PQexec(_conn, "commit");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

- (Boolean) rollback {
    PGresult *pres = PQexec(_conn, "rollback");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

- (void) close {
    PQfinish(_conn);
}

- (void) destroy {
    [self close];
}

@end



// MARK: - PsqlResult

@interface PsqlResult()
-(PsqlResult*) initWithResult:(PGresult *) pres;
@end

@implementation PsqlResult
PGresult *theResult = NULL;

-(PsqlResult *) initWithResult:(PGresult *) result {
    self = [super init];
    theResult = result;
    _rowCount = PQntuples(theResult);
    _columnCount = PQnfields(theResult);
    _curRow = 0;
    if (_rowCount > 0) {
        _isEmpty = false;
    } else {
        _isEmpty = true;
    }
    return self;
}


-(NSString *) getStringWithIndex:(int) colIndex encoding:(NSStringEncoding) encoding {
    NSString *theValue = NULL;
    
    if (colIndex < 0 || colIndex > _columnCount) {
        NSException *exc = [[NSException alloc] initWithName:@"colOutOfRange"
                                                      reason:@"Column out of range"
                                                    userInfo:NULL ];
        [exc raise];
    }
    if (!self.isEOF) {
        if (PQgetisnull(theResult, _curRow, colIndex) != 1) {
            // theValue = [NSString stringWithFormat:@"%s", PQgetvalue(theResult, _curRow, colIndex)];
            theValue = [[NSString alloc] initWithCString:PQgetvalue(theResult, _curRow, colIndex)
                                                encoding:encoding];
        }
    }
    return theValue;
}

-(NSString *) getStringWithIndex:(int) colIndex {
    return [self getStringWithIndex:colIndex encoding:NSUTF8StringEncoding];
}

-(NSString *) getStringWithName:(NSString *) colName encoding:(NSStringEncoding) encoding {
    NSString *theValue = NULL;
    int colNum;
    
    if (!self.isEOF) {
        colNum = PQfnumber(theResult, [colName UTF8String]);
        if (colNum != -1) {
            theValue = [self getStringWithIndex:colNum encoding: encoding];
        }
    }
    return theValue;
}

-(NSString *) getStringWithName:(NSString *) colName {
    return [self getStringWithName:colName encoding: NSUTF8StringEncoding];
}


-(int) getIntWithIndex:(int) colIndex {
    int theInt = 0;
    NSString *valstr = [self getStringWithIndex:colIndex];
    if (valstr != NULL) {
        theInt = [valstr intValue];
    }
    return theInt;
}

-(int) getIntWithName:(NSString *) colName {
    int theInt = 0;
    NSString *valstr = [self getStringWithName:colName];
    if (valstr != NULL) {
        theInt = [valstr intValue];
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
    int len=0;
    size_t binLen=0;
    
    if (colIndex < 0 || colIndex > _columnCount) {
        NSException *exc = [[NSException alloc] initWithName:@"colOutOfRange"
                                                      reason:@"Column out of range"
                                                    userInfo:NULL ];
        [exc raise];
    }
    if (!self.isEOF) {
        if (PQgetisnull(theResult, _curRow, colIndex) != 1) {
            len    = PQgetlength(theResult, _curRow, colIndex);
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
        colNum = PQfnumber(theResult, [colName UTF8String]);
        if (colNum != -1) {
            theData = [self getBytesWithIndex:colNum];
        }
    }
    return theData;
}

-(NSDate *) getDateWithIndex:(int)colIndex format:(NSString *)format {
    NSDate *theDate = nil;
    NSString *valstr = [self getStringWithIndex:colIndex];
    if (valstr != NULL) {
        NSDateFormatter *df = [[NSDateFormatter alloc] init];
        [df setDateFormat:format];
        theDate = [df dateFromString:valstr];
    }
    return theDate;
}

-(NSDate *) getDateWithName:(NSString *)colName format:(NSString *)format {
    NSDate *theDate = nil;
    int colNum = 0;
    
    if (!self.isEOF) {
        colNum = PQfnumber(theResult, [colName UTF8String]);
        if (colNum != -1) {
            theDate = [self getDateWithIndex:colNum format:format];
        }
    }
    return theDate;
}

-(NSDate *) getDateWithIndex:(int)colIndex {
    return [self getDateWithIndex:colIndex format:@"yyyy-MM-dd"];
}

-(NSDate *) getDateWithName:(NSString *)colName {
    return [self getDateWithName:colName format:@"yyyy-MM-dd"];
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

@implementation PsqlStatement : NSObject

NSString *theSqlString = nil;
NSString *stmtName  = nil;
PsqlConnection __weak *theConn;
int numParams = 0;
NSMutableArray *parametres = NULL;


- (PsqlStatement*) initWithString:(NSString*) sqlString
                     pqConnection:(PsqlConnection*) pqConnection {
    self = [super init];
    theConn   = pqConnection;
    stmtName  = [[NSProcessInfo processInfo] globallyUniqueString];
    theSqlString = sqlString;
    _isOK = false;
    return self;
}

- (Boolean) prepare:(NSError **) error {
    PGresult *pres = PQprepare([theConn conn], [stmtName UTF8String],
                               [theSqlString UTF8String], 0, NULL);
    _isOK = false;
    if (pres != NULL) {
        if (PQresultStatus(pres) == PGRES_COMMAND_OK) {
            pres = PQdescribePrepared(theConn.conn, [stmtName UTF8String]);
            numParams = PQnparams(pres);
            parametres = [[NSMutableArray alloc] initWithCapacity:numParams];
            _isOK = true;
            if (error != NULL) *error = nil;
        }
    }
    if (! _isOK) {
        if (error != NULL) {
            NSDictionary *userInfo = @{NSLocalizedDescriptionKey : [theConn getErrorMessage] };
            *error = [[NSError alloc] initWithDomain:errorDomain
                                                code: 2
                                            userInfo: userInfo];
        }
    }
    return _isOK;
}

- (Boolean) setStringParmWithIndex:(int)index value:(NSString *) value {
    if (index < numParams && index >= 0) {
        parametres[index] = value;
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:@"Parameter index out of range"
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}


- (Boolean) setIntParmWithIndex:(int)index value:(int) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%d", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:@"Parameter index out of range"
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}

- (Boolean) setLongParmWithIndex:(int)index value:(long) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%ld", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:@"Parameter index out of range"
                                                    userInfo:NULL ];
        [exc raise];
        return false;
    }
}


- (Boolean) setDoubleParmWithIndex:(int)index value:(double) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%f", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        return true;
    } else {
        NSException *exc = [[NSException alloc] initWithName:@"paramOutOfRange"
                                                      reason:@"Parameter index out of range"
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
        int *paramLengths = calloc(sizeof(int), numParams);
        int *paramFormats = calloc(sizeof(int), numParams);
        for (i=0; i<numParams; i++) {
            paramFormats[i] = 0;
            paramLengths[i] = 0;
            paramValues[i]  = (char *) [parametres[i] UTF8String];
        }
        res = PQexecPrepared([theConn conn], [stmtName UTF8String], numParams, paramValues, paramLengths, paramFormats, 0);
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
                *error = [[NSError alloc] initWithDomain:errorDomain
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
        res = PQexecPrepared([theConn conn], [stmtName UTF8String], numParams, paramValues, paramLengths, paramFormats, 0);
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
                *error = [[NSError alloc] initWithDomain:errorDomain
                                                    code: 3
                                                userInfo: userInfo];
            }
        }
    }
    return pr;
}

- (void) close {
    stmtName = nil;
    parametres = nil;
}
@end

