# csv2postgresql

CSVファイルを読み込んで、PostgreSQLにロードするツールです。

事前にテーブルを作成しておく必要はありません。CSVファイルのヘッダに記載のフィールド名を元にテーブルが作成されます。
既にテーブルが存在した場合には、そのテーブルに対してロードします。

## 利用方法

実行にはJava(JDK8以上)が必要となります。

下記から最新の実行ファイル(`csv2postgresql-x.x.x-all.jar`)を入手します。

* https://github.com/onozaty/csv2postgresql/releases/latest

入手したjarファイルを指定してアプリケーションを実行します。

```
java -jar csv2postgresql-1.0.0-all.jar config.properties table1 data.csv
```

引数は下記の通りです。
1. 設定ファイルパス
1. テーブル名
1. CSVファイルパス

実行すると、下記のように処理したレコード件数、かかった時間が出力されます。

```
Loading...
Loading is completed. (Number of records: 100,000 / Elapsed millsecods: 322)
```

### 設定ファイル

設定ファイルには、DBの接続先情報と、CSVファイルのエンコーディングを記載します。

* `database.url` JDBC接続文字列
* `database.user` DBユーザ名
* `database.password` DBパスワード
* `csv.encoding` CSVファイルのエンコーディング

以下は例です。

```
database.url=jdbc:postgresql://192.168.33.10:5432/testdb
database.user=user1
database.password=pass1
csv.encoding=UTF-8
```

### テーブル名

ロード先のテーブル名を指定します。

テーブルが存在しなかった場合、新規にテーブルを作成します。この際、各カラムは`text`型として作成されます。

### CSVファイル

CSVファイルのヘッダは必須です。ヘッダに記載のフィールド名を使って、ロード先のテーブルのカラムとマッピングします。
なお、英数字以外の文字が指定されていた場合、アンダースコア(`_`)に置換されます。

たとえば`User Name`というフィールドがあった場合、DBのカラムとしては`user_name`にマッピングされます。

## サンプル

PostgreSQLを起動するためのVagrant環境と、設定ファイルとCSVファイルのサンプルが用意してあります。
これらを使うことによって、簡単に本ツールを試せます。

`vagrant`フォルダにて`vagrant up`を実行すると、PostgreSQL11をインストールした仮想環境(192.168.33.10)を立ち上げます。

`sample`フォルダ配下の設定ファイルとCSVファイルを利用してロードを行います。
```
java -jar csv2postgresql-1.0.0-all.jar sample/config.properties test_table sample/sample-100000.csv
```

## ビルド方法

ソースコードからビルドして利用する場合、Java(JDK8以上)がインストールされた環境で、下記コマンドでアプリケーションをビルドします。

```
gradlew shadowJar
```

`build/libs/csv2postgresql-x.x.x-all.jar`という実行ファイルが出来上がります。(`x.x.x`はバージョン番号)
