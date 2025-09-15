const _support = require("./support");
const decrypt = require("../lib/decrypt");
const bigquery = require("../lib/bigquery");

const stubInsert = (fn) => {
  sinon.stub(bigquery, "client").resolves({
    dataset: (ds) => {
      return {
        table: (tbl) => {
          return {
            insert: async (rows, opts) => fn(ds, tbl, rows, opts),
          };
        },
      };
    },
  });
};

describe("bigquery", () => {
  it("detects encrypted looking private keys", () => {
    sinon.stub(decrypt, "decryptAws").resolves("okay");
    return bigquery.key(true).then((_key) => {
      expect(decrypt.decryptAws).not.to.have.been.called;
      process.env.BQ_PRIVATE_KEY = "this-looks-encrypted";
      return bigquery.key(true).then((_key2) => {
        expect(decrypt.decryptAws).to.have.been.called;
      });
    });
  });

  it("short circuits when inserting nothing", () => {
    return bigquery.insert("thedataset", "thetable", []).then((count) => {
      expect(count).to.equal(0);
    });
  });

  describe("with a mocked dataset", () => {
    let inserted, insertOpts;
    beforeEach(() => {
      inserted = {};
      stubInsert((_ds, tbl, rows, opts) => {
        inserted[tbl] = rows;
        insertOpts = opts;
      });
    });

    it("inserts raw rows", () => {
      return bigquery
        .insert("thedataset", "thetable", [{ id: "foo" }, { id: "bar" }])
        .then((count) => {
          expect(count).to.equal(2);
          expect(inserted).to.have.keys("thetable");
          expect(inserted.thetable.length).to.equal(2);
          expect(inserted.thetable[0].id).to.equal("foo");
          expect(inserted.thetable[1].id).to.equal("bar");
          expect(insertOpts.raw).to.equal(true);
        });
    });
  });

  describe("with an insert error", () => {
    let thrown;
    beforeEach(() => {
      thrown = 0;
      stubInsert((_ds, _tbl, _rows, _opts) => {
        thrown++;
        throw new Error(`err${thrown}`);
      });
    });

    it("retries failures 2 times", () => {
      return bigquery.insert("thedataset", "thetable", [{ id: "foo" }, { id: "bar" }]).then(
        (_count) => {
          throw new Error("Should have gotten an error");
        },
        (err) => {
          expect(thrown).to.equal(3);
          expect(err.message).to.equal("err3");
        },
      );
    });
  });
});
