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

- (PsqlConnection *) init;
- (Boolean) connectWithUrl:(NSString *)url
                  userName:(NSString *)userName
                  password:(NSString *)password
                     error:(NSError **)error;
- (NSString *) getErrorMessage;
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

-(NSString *) getStringWithIndex:(int) colIndex;
-(NSString *) getStringWithName:(NSString *) colName;

-(int) getIntWithIndex:(int) colIndex;
-(int) getIntWithName:(NSString *) colName;

-(NSNumber *) getNumberWithIndex:(int) colIndex;
-(NSNumber *) getNumberWithName:(NSString *) colName;

-(NSMutableData *) getBytesWithIndex:(int) colIndex;
-(NSMutableData *) getBytesWithName:(NSString *) colName;

-(NSDate *) getDateWithIndex:(int) colIndex format:(NSString*) format;
-(NSDate *) getDateWithName:(NSString *) colName format:(NSString*) format;

-(NSDate *) getDateWithIndex:(int) colIndex;
-(NSDate *) getDateWithName:(NSString *) colName;

-(void) close;
@end

@interface PsqlStatement : NSObject
@property (readonly) Boolean isOK;

- (PsqlStatement*) initWithString:(NSString*) sqlString
                     pqConnection:(PsqlConnection*) pqConnection;
- (Boolean) prepare:(NSError **) error;
- (Boolean) setStringParmWithIndex:(int)index value:(NSString *) value;
- (Boolean) setIntParmWithIndex:(int)index value:(int) value;
- (Boolean) setLongParmWithIndex:(int)index value:(long) value;
- (Boolean) setDoubleParmWithIndex:(int)index value:(double) value;

- (int) execute:(NSError **)error;
- (PsqlResult *) executeQuery:(NSError**)error;
- (void) close;
@end
