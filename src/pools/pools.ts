import { Signer } from '@reef-defi/evm-provider';
import { Contract } from 'ethers';
import { ensure, uniqueCombinations } from '../utils';
import {getReefswapFactory} from "../utils/rpc";
import {reefTokenWithAmount, Token} from "../token/token";
import {Pool} from "../token/pool";
import {ReefswapPair} from "../token/abi/ReefswapPair";
import {combineLatest, map, switchMap} from "rxjs";
import {apolloClientInstance$, zenToRx} from "../graphql";
import {selectedSigner$} from "../appState/account/selectedSigner";
import {currentProvider$} from "../appState/providerState";
import {gql} from "@apollo/client";
import {AVAILABLE_REEF_POOLS_GQL} from "../graphql/availablePools";

const EMPTY_ADDRESS = '0x0000000000000000000000000000000000000000';

const findPoolTokenAddress = async (
  address1: string,
  address2: string,
  signer: Signer,
  factoryAddress: string,
): Promise<string> => {
  const reefswapFactory = getReefswapFactory(factoryAddress, signer);
  const address = await reefswapFactory.getPair(address1, address2);
  return address;
};

export const loadPool = async (
  token1: Token,
  token2: Token,
  signer: Signer,
  factoryAddress: string,
): Promise<Pool> => {
  const address = await findPoolTokenAddress(
    token1.address,
    token2.address,
    signer,
    factoryAddress,
  );
  ensure(address !== EMPTY_ADDRESS, 'Pool does not exist!');
  const contract = new Contract(address, ReefswapPair, signer);

  const decimals = await contract.decimals();
  const reserves = await contract.getReserves();
  const totalSupply = await contract.totalSupply();
  const liquidity = await contract.balanceOf(await signer.getAddress());

  const address1 = await contract.token1();

  const [finalReserve1, finalReserve2] = token1.address !== address1
    ? [reserves[0], reserves[1]]
    : [reserves[1], reserves[0]];

  const tokenBalance1 = finalReserve1.mul(liquidity).div(totalSupply);
  const tokenBalance2 = finalReserve2.mul(liquidity).div(totalSupply);

  return {
    poolAddress: address,
    decimals: parseInt(decimals, 10),
    reserve1: finalReserve1.toString(),
    reserve2: finalReserve2.toString(),
    totalSupply: totalSupply.toString(),
    userPoolBalance: liquidity.toString(),
    token1: { ...token1, balance: tokenBalance1 },
    token2: { ...token2, balance: tokenBalance2 },
  };
};

export const loadPools = async (
  tokens: Token[],
  signer: Signer,
  factoryAddress: string,
): Promise<Pool[]> => {
  const tokenCombinations = uniqueCombinations(tokens);
  const pools: Pool[] = [];
  for (let index = 0; index < tokenCombinations.length; index += 1) {
    try {
      const [token1, token2] = tokenCombinations[index];
      /* eslint-disable no-await-in-loop */
      const pool = await loadPool(token1, token2, signer, factoryAddress);
      /* eslint-disable no-await-in-loop */
      pools.push(pool);
    } catch (e) {}
  }
  return pools;
};
