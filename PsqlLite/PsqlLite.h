//
//  PsqlLite.h
//  PsqlLite
//
//  Created by Jordi Guillaumes Pons on 27/3/16.
//  Copyright © 2016 Jordi Guillaumes Pons. All rights reserved.
//

#import <Foundation/Foundation.h>

//! Project version number for PsqlLite.
FOUNDATION_EXPORT double PsqlLiteVersionNumber;

//! Project version string for PsqlLite.
FOUNDATION_EXPORT const unsigned char PsqlLiteVersionString[];

// In this header, you should import all the public headers of your framework using statements like #import <PsqlLite/PublicHeader.h>

@interface PsqlConnection : NSObject

- (nonnull PsqlConnection *) init;
- (Boolean) connectWithUrl:(nonnull NSString *)url
                  userName:(nonnull NSString *)userName
                  password:(nonnull NSString *)password
                     error:(NSError **)error;
- (nonnull NSString *) getErrorMessage;
- (Boolean) isConnected;
- (Boolean) begin;
- (Boolean) commit;
- (Boolean) rollback;
- (void) close;
- (void) destroy;
@end

@interface PsqlResult : NSObject
@property (readonly) int rowCount;
@property (readonly) int columnCount;
@property (readonly) int curRow;
@property (readonly) Boolean isBOF;
@property (readonly) Boolean isEOF;
@property (readonly) Boolean isEmpty;

-(Boolean) nextRow;
-(Boolean) firstRow;
-(Boolean) lastRow;

-(nullable NSString *) getStringWithIndex:(int) colIndex;
-(nullable NSString *) getStringWithName:(nonnull NSString *) colName;

-(int) getIntWithIndex:(int) colIndex;
-(int) getIntWithName:(nonnull NSString *) colName;

-(nullable NSNumber *) getNumberWithIndex:(int) colIndex;
-(nullable NSNumber *) getNumberWithName:(nonnull NSString *) colName;

-(nullable NSMutableData *) getBytesWithIndex:(int) colIndex;
-(nullable NSMutableData *) getBytesWithName:(nonnull NSString *) colName;

-(nullable NSDate *) getDateWithIndex:(int) colIndex format:(nonnull NSString*) format;
-(nullable NSDate *) getDateWithName:(nonnull NSString *) colName format:(nonnull NSString*) format;

-(nullable NSDate *) getDateWithIndex:(int) colIndex;
-(nullable NSDate *) getDateWithName:(nonnull NSString *) colName;

-(void) close;
@end

@interface PsqlStatement : NSObject
@property (readonly) Boolean isOK;

- (nonnull PsqlStatement*) initWithString:(nonnull NSString*) sqlString
                             pqConnection:(nonnull PsqlConnection*) pqConnection;
- (Boolean) prepare:(NSError **) error;
- (Boolean) setStringParmWithIndex:(int)index value:(nullable NSString *) value;
- (Boolean) setIntParmWithIndex:(int)index value:(int) value;
- (Boolean) setLongParmWithIndex:(int)index value:(long) value;
- (Boolean) setDoubleParmWithIndex:(int)index value:(double) value;
- (nonnull NSString *) getStatementName;

- (int) execute:(NSError **)error;
- (nullable PsqlResult *) executeQuery:( NSError  **)error;
- (void) close;
@end
