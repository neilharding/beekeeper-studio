export default {
  name: "20260216_add_athena_options",
  async run(runner) {
    const queries = [
      `ALTER TABLE saved_connection ADD COLUMN athenaOptions text not null default '{}'`,
      `ALTER TABLE used_connection ADD COLUMN athenaOptions text not null default '{}'`,
    ];
    for (let i = 0; i < queries.length; i++) {
      const query = queries[i];
      await runner.query(query);
    }
  }
}
