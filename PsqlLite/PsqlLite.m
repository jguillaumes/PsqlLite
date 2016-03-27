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

@implementation PsqlConnection : NSObject

PGconn *conn;


- (PsqlConnection *) init {
    self = [super init];
    conn = NULL;
    return self;
}

- (void) connectWithUrl:(NSString*)url
               userName:(NSString*)userName
               password:(NSString*)password {
    NSString *theUrl = [ NSString stringWithFormat:@"%@?user=%@&password=%@", url, userName, password] ;
    conn = PQconnectdb([theUrl UTF8String]);
}

- (NSString *) getErrorMessage {
    NSString *msg = [NSString stringWithFormat: @"%s", PQerrorMessage(conn)];
    return msg;
}

- (Boolean) isConnected {
    return PQstatus(conn) == CONNECTION_OK;
}

- (Boolean) begin {
    PGresult *pres = PQexec(conn, "begin");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

- (Boolean) commit {
    PGresult *pres = PQexec(conn, "commit");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

- (Boolean) rollback {
    PGresult *pres = PQexec(conn, "rollback");
    return (PQresultStatus(pres) == PGRES_COMMAND_OK);
}

- (void) close {
    PQfinish(conn);
}

- (PGconn *) getConn {
    return conn;
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

NSString *stmtName = NULL;
PGconn *theConn;
int numParams = 0;
NSMutableArray *parametres = NULL;


- (PsqlStatement*) initWithString:(NSString*) sqlString
                     pqConnection:(PsqlConnection*) pqConnection {
    self = [super init];
    theConn = pqConnection.getConn;
    stmtName = [[NSProcessInfo processInfo] globallyUniqueString];
    PGresult *pres = PQprepare(theConn, [stmtName UTF8String],
                               [sqlString UTF8String], 0, NULL);
    if (pres != NULL) {
        if (PQresultStatus(pres) == PGRES_COMMAND_OK) {
            pres = PQdescribePrepared(theConn, [stmtName UTF8String]);
            numParams = PQnparams(pres);
            parametres = [[NSMutableArray alloc] initWithCapacity:numParams];
            _isOK = true;
        }
    }
    return self;
}

- (Boolean) setStringParmWithIndex:(int)index value:(NSString *) value {
    if (index < numParams && index >= 0) {
        parametres[index] = value;
        return true;
    } else {
        return false;
    }
}


- (Boolean) setIntParmWithIndex:(int)index value:(int) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%d", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        return true;
    } else {
        return false;
    }
}

- (Boolean) setLongParmWithIndex:(int)index value:(long) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%ld", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        return true;
    } else {
        return false;
    }
}


- (Boolean) setDoubleParmWithIndex:(int)index value:(double) value {
    NSString *valstr = [[NSString alloc] initWithFormat:@"%f", value];
    
    if (index < numParams && index >= 0) {
        parametres[index] = valstr;
        return true;
    } else {
        return false;
    }
}


- (PsqlResult *) execute {
    PsqlResult *pr = NULL;
    PGresult *res = NULL;
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
        res = PQexecPrepared(theConn, [stmtName UTF8String], numParams, paramValues, paramLengths, paramFormats, 0);
        pr = [[PsqlResult alloc] initWithResult:res];
        free(paramValues);
        free(paramLengths);
        free(paramFormats);
        // PQprint(stderr, res, &printopt);
        return pr;
    } else {
        return NULL;
    }
}

- (void) close {
    stmtName = nil;
    parametres = nil;
}
@end

