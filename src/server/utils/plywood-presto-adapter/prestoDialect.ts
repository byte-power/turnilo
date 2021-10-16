import { Duration, Timezone } from 'chronoshift';
import { PlyType, SQLDialect } from 'plywood';
// import { SQLDialect } from 'plywood/build/dialect/baseDialect';

export class PrestoDialect extends SQLDialect {
  static TIME_BUCKETING: Record<string, string> = {
    PT1S: 'second',
    PT1M: 'minute',
    PT1H: 'hour',
    P1D: 'day',
    P1W: 'week',
    P1M: 'month',
    P3M: 'quarter',
    P1Y: 'year',
  };

  static TIME_PART_TO_FUNCTION: Record<string, string> = {
    SECOND_OF_MINUTE: 'SECOND($$)',
    SECOND_OF_HOUR: '(MINUTE($$)*60+SECOND($$))',
    SECOND_OF_DAY: '((HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
    SECOND_OF_WEEK: '(((DOW($$)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
    SECOND_OF_MONTH: '((((DAY($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
    SECOND_OF_YEAR: '((((DOY($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',

    MINUTE_OF_HOUR: 'MINUTE($$)',
    MINUTE_OF_DAY: 'HOUR($$)*60+MINUTE($$)',
    MINUTE_OF_WEEK: '(DOW($$)*24)+HOUR($$)*60+MINUTE($$)',
    MINUTE_OF_MONTH: '((DAY($$)-1)*24)+HOUR($$)*60+MINUTE($$)',
    MINUTE_OF_YEAR: '((DOY($$)-1)*24)+HOUR($$)*60+MINUTE($$)',

    HOUR_OF_DAY: 'HOUR($$)',
    HOUR_OF_WEEK: '(DOW($$)*24+HOUR($$))',
    HOUR_OF_MONTH: '((DAY($$)-1)*24+HOUR($$))',
    HOUR_OF_YEAR: '((DOY($$)-1)*24+HOUR($$))',

    DAY_OF_WEEK: 'DOW($$)',
    DAY_OF_MONTH: 'DAY($$)',
    DAY_OF_YEAR: 'DOY($$)',

    //WEEK_OF_MONTH: ???
    WEEK_OF_YEAR: 'WEEK($$)', // ToDo: look into mode (https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_week)

    MONTH_OF_YEAR: 'MONTH($$)',
    YEAR: 'YEAR($$)',
  };

  static CAST_TO_FUNCTION: Record<string, Record<string, string>> = {
    TIME: {
      NUMBER: 'CAST(FROM_UNIXTIME(CAST($$ AS DOUBLE) / 1000) AS TIMESTAMP)',
    },
    NUMBER: {
      TIME: 'CAST(TO_UNIXTIME(CAST($$ AS TIMESTAMP) * 1000) AS bigint)',
      STRING: 'CAST($$ AS BIGINT)',
    },
    STRING: {
      NUMBER: 'CAST($$ AS varchar)',
    },
  };

  constructor() {
    super();
  }

  public emptyGroupBy(): string {
    return "GROUP BY ''=''";
  }

  public timeToSQL(date: Date): string {
    if (!date) return this.nullConstant();
    return `CAST('${this.dateToSQLDateString(date)}' AS TIMESTAMP)`;
  }

  public concatExpression(a: string, b: string): string {
    return `CONCAT(${a},${b})`;
  }

  public containsExpression(a: string, b: string): string {
    return `POSITION(${a} IN ${b})>0`;
  }

  public regexpExpression(expression: string, regexp: string): string {
    return `(${expression} ~ '${regexp}')`; // ToDo: escape this.regexp
  }

  public castExpression(inputType: PlyType, operand: string, cast: string): string {
    let castFunction = PrestoDialect.CAST_TO_FUNCTION[cast][inputType];
    if (!castFunction)
      throw new Error(`unsupported cast from ${inputType} to ${cast} in Presto dialect`);
    return castFunction.replace(/\$\$/g, operand);
  }

  public utcToWalltime(operand: string, timezone: Timezone): string {
    if (timezone.isUTC()) return operand;
    return `(${operand} AT TIME ZONE 'UTC' AT TIME ZONE '${timezone}')`;
  }

  public walltimeToUTC(operand: string, timezone: Timezone): string {
    if (timezone.isUTC()) return operand;
    return `(${operand} AT TIME ZONE '${timezone}' AT TIME ZONE 'UTC')`;
  }

  public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
    let bucketFormat = PrestoDialect.TIME_BUCKETING[duration.toString()];
    if (!bucketFormat) throw new Error(`unsupported duration '${duration}'`);
    return this.walltimeToUTC(
      '${bucketFormat}(${this.utcToWalltime(operand, timezone)})',
      timezone,
    );
  }

  public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return this.timeFloorExpression(operand, duration, timezone);
  }

  public timePartExpression(operand: string, part: string, timezone: Timezone): string {
    let timePartFunction = PrestoDialect.TIME_PART_TO_FUNCTION[part];
    if (!timePartFunction) throw new Error(`unsupported part ${part} in Postgres dialect`);
    return timePartFunction.replace(/\$\$/g, this.utcToWalltime(operand, timezone));
  }

  public timeShiftExpression(
    operand: string,
    duration: Duration,
    // step: int,
    timezone: Timezone,
  ): string {
    // if (step === 0) return operand;

    // // https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_date-add
    // let sqlFn = step > 0 ? 'DATE_ADD(' : 'DATE_SUB(';
    // let spans = duration.multiply(Math.abs(step)).valueOf();
    // if (spans.week) {
    //   return sqlFn + `'WEEK', ` + String(spans.week) + ', ' + operand + ')';
    // }
    // if (spans.year || spans.month) {
    //   let expr = String(spans.year || 0) + '-' + String(spans.month || 0);
    //   operand = sqlFn + operand + ", INTERVAL '" + expr + "')";
    // }
    // if (spans.day || spans.hour || spans.minute || spans.second) {
    //   let expr =
    //     String(spans.day || 0) +
    //     ' ' +
    //     [spans.hour || 0, spans.minute || 0, spans.second || 0].join(':');
    //   operand = sqlFn + operand + ", INTERVAL '" + expr + "' DAY_SECOND)";
    // }
    return operand;
  }

  public extractExpression(operand: string, regexp: string): string {
    return `(SELECT (REGEXP_EXTRACT(${operand}, '${regexp}')))`;
  }

  public indexOfExpression(str: string, substr: string): string {
    return `POSITION(${substr} IN ${str}) - 1`;
  }
}