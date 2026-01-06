export const log = (msg: string, ...args: any) => {
  console.log(`[${new Date().toISOString()}] ${msg}`, ...args);
}

export const error = (msg: string, ...args: any) => {
  console.error(`[${new Date().toISOString()}] ${msg}`, ...args);
}
