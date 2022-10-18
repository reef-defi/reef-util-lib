import {ContractInterface} from 'ethers';
import {Provider} from '@reef-defi/evm-provider';
import {ApolloClient} from '@apollo/client';
import type {Signer as InjectedSigningKey} from '@polkadot/api/types';
import {AccountJson} from '@reef-defi/extension-base/background/types';
import type {
  InjectedAccount as InjectedAccountReef,
  InjectedAccountWithMeta as InjectedAccountWithMetaReef
} from '@reef-defi/extension-inject/types';
import type {InjectedAccount, InjectedAccountWithMeta,} from '@polkadot/extension-inject/types';
import {ContractType, REEF_ADDRESS, reefTokenWithAmount, Token, TokenWithAmount} from '../../token/token';
import {Network,} from '../../network/network';
import {ERC20} from '../../token/abi/ERC20';
import {ERC721Uri} from '../../token/abi/ERC721Uri';
import {ERC1155Uri} from '../../token/abi/ERC1155Uri';
import {calculateTokenPrice, calculateTokenPrice_fbk} from '../../utils';
import {apolloClientSubj, setApolloUrls} from '../../graphql';
import {ipfsUrlResolverFn} from '../../token/nftUtil';
import {ReefSigner} from "../../account/ReefAccount";
import {Pool} from "../../token/pool";
import {FeedbackDataModel, FeedbackStatus, FeedbackStatusCode, toFeedbackDM} from "../model/feedbackDataModel";

export let _NFT_IPFS_RESOLVER_FN: ipfsUrlResolverFn|undefined;

export const setNftIpfsResolverFn = (val?: ipfsUrlResolverFn) => {
  _NFT_IPFS_RESOLVER_FN = val;
};

export const toPlainString = (num: number): string => `${+num}`.replace(
    /(-?)(\d*)\.?(\d*)e([+-]\d+)/,
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    (a, b, c, d, e) => (e < 0
        ? `${b}0.${Array(1 - e - c.length).join('0')}${c}${d}`
        : b + c + d + Array(e - d.length + 1).join('0')),
);

export const sortReefTokenFirst = (tokens: FeedbackDataModel<Token>[]): FeedbackDataModel<Token>[] => {
  const reefTokenIndex = tokens.findIndex((t: FeedbackDataModel<Token>) => t.data.address === REEF_ADDRESS);
  if (reefTokenIndex > 0) {
    return [tokens[reefTokenIndex], ...tokens.slice(0, reefTokenIndex), ...tokens.slice(reefTokenIndex + 1, tokens.length)];
  }
  return tokens;
};

export const combineTokensDistinct = ([tokens1, tokens2]: [
  Token[]|null,
  Token[]
]): Token[] => {
  if(!tokens1){
    tokens1 = [];
  }
  const combinedT = [...tokens1];
  // console.log('COMBINED=', combinedT);
  tokens2.forEach((vT: Token) => (!combinedT.some((cT) => cT.address === vT.address)
    ? combinedT.push(vT)
    : null));
  // console.log('1111COMBINED=', combinedT);
  return combinedT;
};

export const toTokensWithPrice = ([tokens, reefPrice, pools]: [
  Token[]|null,
  number,
  Pool[]
]): TokenWithAmount[] => tokens?tokens.map(
  (token) => ({
    ...token,
    price: calculateTokenPrice(token, pools, reefPrice),
  } as TokenWithAmount),
):[];

export const toTokensWithPrice_fbk = ([tokens, reefPrice, pools]: [
  FeedbackDataModel<Token>[]|null,
  FeedbackDataModel<number>,
  FeedbackDataModel<Pool|null>[]
]): FeedbackDataModel<TokenWithAmount>[] => tokens?tokens.map(
  (token_fbk) => {
    if (token_fbk.hasStatus(FeedbackStatusCode.COMPLETE_DATA)) {

      const priceFDM = calculateTokenPrice_fbk(token_fbk.data, pools, reefPrice);
      token_fbk.setStatus([{...priceFDM.getStatus(), propName: 'price', message:'Price set'}]);
      (token_fbk.data as TokenWithAmount).price = priceFDM.data;
      console.log("PRICE SET=",token_fbk.data.address, token_fbk.getStatus());
      return token_fbk as FeedbackDataModel<TokenWithAmount>;
    }
    (token_fbk.data as TokenWithAmount).price = 0;
    return token_fbk as FeedbackDataModel<TokenWithAmount>;
  },
):[];

export const getGQLUrls = (network: Network): { ws: string; http: string }|undefined => {
  if (!network.graphqlUrl) {
    return undefined;
  }
  const ws = network.graphqlUrl.startsWith('http')
    ? network.graphqlUrl.replace('http', 'ws')
    : network.graphqlUrl;
  const http = network.graphqlUrl.startsWith('ws')
    ? network.graphqlUrl.replace('ws', 'http')
    : network.graphqlUrl;
  return { ws, http };
};

export interface State {
  loading: boolean;
  signers?: ReefSigner[];
  provider?: Provider;
  network?: Network;
  error?: any; // TODO!
}

export interface StateOptions {
  network?: Network;
  signers?: ReefSigner[];
  client?: ApolloClient<any>;
  jsonAccounts?:{accounts: AccountJson[] | InjectedAccountWithMeta[] | InjectedAccountWithMetaReef[], injectedSigner: InjectedSigningKey}
  ipfsHashResolverFn?: ipfsUrlResolverFn;
}

export function initApolloClient(selectedNetwork?: Network, client?: ApolloClient<any>) {
  if (selectedNetwork) {
    if (!client) {
      const gqlUrls = getGQLUrls(selectedNetwork);
      if (gqlUrls) {
        setApolloUrls(gqlUrls);
      }
    } else {
      apolloClientSubj.next(client);
    }
  }
}

export const toInjectedAccountsWithMeta = (injAccounts: InjectedAccount[] | InjectedAccountReef[], extensionSourceName: string): InjectedAccountWithMeta[] | InjectedAccountWithMetaReef[]=>{
  return injAccounts.map(acc => ({
    address: acc.address,
    meta: {source: extensionSourceName}
  } as InjectedAccountWithMeta | InjectedAccountWithMetaReef));
}
